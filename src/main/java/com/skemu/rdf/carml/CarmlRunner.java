package com.skemu.rdf.carml;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.engine.RmlMapper;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.rdf_mapper.util.ImmutableCollectors;
import com.taxonic.carml.util.RmlMappingLoader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriterRegistry;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.WriterConfig;
import org.eclipse.rdf4j.rio.helpers.BasicWriterSettings;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Component
@Order(value = Ordered.LOWEST_PRECEDENCE)
public class CarmlRunner implements CommandLineRunner, InitializingBean {
	
	private static final Logger LOGGER = LogManager.getLogger();
	
	private static final String MAPPING_FILE_OPTION = "m";
	private static final String MAPPING_FILE_OPTION_LONG = "mapping";
	private static final String MAPPING_FORMAT_OPTION = "f";
	private static final String MAPPING_FORMAT_OPTION_LONG = "format";
	private static final String RELATIVE_SOURCE_LOCATION_OPTION = "rsl";
	private static final String RELATIVE_SOURCE_LOCATION_OPTION_LONG = "rel-src-loc";
	private static final String FUNCTION_JAR_OPTION = "j";
	private static final String FUNCTION_JAR_OPTION_LONG = "jars";
	private static final String FUNCTION_OPTION = "fn";
	private static final String FUNCTION_OPTION_LONG = "functions";
	private static final String OUTPUT_FILE_OPTION = "o";
	private static final String OUTPUT_FILE_OPTION_LONG = "output";
	private static final String OUTPUT_FORMAT_OPTION = "of";
	private static final String OUTPUT_FORMAT_OPTION_LONG = "outformat";
	private static final String OUTPUT_NAMESPACE_OPTION = "p";
	private static final String OUTPUT_NAMESPACE_OPTION_LONG = "prefix";
	private static final String OUTPUT_CONTEXT_OPTION = "c";
	private static final String OUTPUT_CONTEXT_OPTION_LONG = "context";
	
	private Options options;
	private CommandLineParser cmdParser;
	private CommandLine cmd;
	private HelpFormatter helpFormatter;
	
	@Override
	public void run(String... args) throws Exception {
		try {
			cmd = cmdParser.parse(options, args);
		} catch(ParseException e) {
			help();
			System.exit(1);
		}
		
		RmlMapper mapper = prepareMapper(cmd);
		Set<TriplesMap> mapping = loadMapping(cmd);
		LOGGER.info("Executing mapping ...");
		Model model = mapper.map(mapping);
		
		getOutputNamespaceDeclarations(cmd).forEach(model::setNamespace);
		
		String outputPath = cmd.getOptionValue(OUTPUT_FILE_OPTION);
		LOGGER.info("Writing output to {} ...", outputPath);
		writeToFile(model, outputPath, loadOutputRdfFormat(cmd));
	}
	
	private void help() {
		helpFormatter.printHelp("carml", options);
	}
	
	private RmlMapper prepareMapper(CommandLine cmd) {
		Objects.requireNonNull(cmd);
		
		RmlMapper.Builder mapperBuilder = RmlMapper.newBuilder();
		
		if (cmd.hasOption(FUNCTION_JAR_OPTION) && cmd.hasOption(FUNCTION_OPTION)) {
			Set<String> fnClasses = ImmutableSet.copyOf(cmd.getOptionValues(FUNCTION_OPTION));
			
			Set<File> fnJars = Arrays.asList(cmd.getOptionValues(FUNCTION_JAR_OPTION)).stream()
					.map(File::new)
					.collect(ImmutableCollectors.toImmutableSet());
			
			LOGGER.debug("Loading transformation functions ...");
			Set<Object> functions = JarFunctionLoader.load(fnClasses, fnJars);
			functions.forEach(mapperBuilder::addFunctions);	
		}
		
		if (cmd.hasOption(RELATIVE_SOURCE_LOCATION_OPTION)) {
			mapperBuilder.fileResolver(Paths.get(cmd.getOptionValue(RELATIVE_SOURCE_LOCATION_OPTION)));
		}
		
		return mapperBuilder.build();
	}
	
	private Set<TriplesMap> loadMapping(CommandLine cmd) {
		Objects.requireNonNull(cmd);
		
		String pathToFile = cmd.getOptionValue(MAPPING_FILE_OPTION);
		Path path = Paths.get(pathToFile);
		LOGGER.info("Loading mapping from {} ...", pathToFile);
		return RmlMappingLoader.build().load(path, loadMappingRdfFormat(cmd, pathToFile));
	}
		
	private RDFFormat loadMappingRdfFormat(CommandLine cmd, String pathToFile) {
		Objects.requireNonNull(cmd);
		Objects.requireNonNull(pathToFile);
		
		if (cmd.hasOption(MAPPING_FORMAT_OPTION)) {
			String format = cmd.getOptionValue(MAPPING_FORMAT_OPTION);
			return determineRdfFormat(format).orElse(RDFFormat.TURTLE);
		}
		LOGGER.info("Defaulting to turtle format ...");
		return RDFFormat.TURTLE;
		
	}
	
	private RDFFormat loadOutputRdfFormat(CommandLine cmd) {
		Objects.requireNonNull(cmd);
		if (cmd.hasOption(OUTPUT_FORMAT_OPTION)) {
			String format = cmd.getOptionValue(OUTPUT_FORMAT_OPTION);
			return determineRdfFormat(format).orElse(RDFFormat.NQUADS);
		}
		LOGGER.info("Defaulting to N-Quads format ...");
		return RDFFormat.NQUADS;
	}
	
	private Optional<RDFFormat> determineRdfFormat(String format) {
		Objects.requireNonNull(format);
		
		return RDFWriterRegistry.getInstance().getKeys().stream()
		.filter(f -> f.getDefaultFileExtension().equals(format))
		.findFirst();
	}
	
	private Set<Namespace> getOutputNamespaceDeclarations(CommandLine cmd) {
		Objects.requireNonNull(cmd);
		Set<Namespace> namespaces = new HashSet<>();
		if (cmd.hasOption(OUTPUT_NAMESPACE_OPTION)) {
			String[] prefixes = cmd.getOptionValues(OUTPUT_NAMESPACE_OPTION);
			namespaces.addAll(ContextLoader.getNamespaces(prefixes));
		}
		
		if (cmd.hasOption(OUTPUT_CONTEXT_OPTION)) {
			File contextFile = new File(cmd.getOptionValue(OUTPUT_CONTEXT_OPTION));
			namespaces.addAll(ContextLoader.getNamespaces(contextFile));
		}
		
		return ImmutableSet.copyOf(namespaces);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		options = new Options();
		cmdParser = new DefaultParser();
		helpFormatter = new HelpFormatter();
		
		Option mappingOption =
				Option.builder(MAPPING_FILE_OPTION)
				.longOpt(MAPPING_FILE_OPTION_LONG)
				.argName(MAPPING_FILE_OPTION_LONG)
				.hasArg()
				.required()
				.desc("Mapping file")
				.build();		
		options.addOption(mappingOption);
		
		Option formatOption =
				Option.builder(MAPPING_FORMAT_OPTION)
				.longOpt(MAPPING_FORMAT_OPTION_LONG)
				.argName(MAPPING_FORMAT_OPTION_LONG)
				.hasArg()
				.desc("Mapping file RDF format:\r\n" + 
						"ttl (text/turtle), \r\n" + 
						"nt (application/n-triples), \r\n" + 
						"nq (application/n-quads), \r\n" + 
						"rdf (application/rdf+xml), \r\n" + 
						"jsonld (application/ld+json), \r\n" + 
						"trig (application/trig), \r\n" + 
						"n3 (text/n3), \r\n" + 
						"trix (application/trix), \r\n" + 
						"brf (application/x-binary-rdf), \r\n" + 
						"rj (application/rdf+json)\r\n")
				.build();		
		options.addOption(formatOption);
		
		Option relSrcLocOption =
				Option.builder(RELATIVE_SOURCE_LOCATION_OPTION)
				.longOpt(RELATIVE_SOURCE_LOCATION_OPTION_LONG)
				.argName(RELATIVE_SOURCE_LOCATION_OPTION_LONG)
				.hasArg()
				.desc("Specify directory to use to find relative logical source in mapping file")
				.build();		
		options.addOption(relSrcLocOption);
		
		Option functionJarsOption =
				Option.builder(FUNCTION_JAR_OPTION)
				.longOpt(FUNCTION_JAR_OPTION_LONG)
				.argName(FUNCTION_JAR_OPTION_LONG)
				.hasArgs()
				.desc("Jar files conatining transformation functions to add to mapper")
				.build();		
		options.addOption(functionJarsOption);
		
		Option functionsOption =
				Option.builder(FUNCTION_OPTION)
				.longOpt(FUNCTION_OPTION_LONG)
				.argName(FUNCTION_OPTION_LONG)
				.hasArgs()
				.desc("Transformation function classes from -"+ FUNCTION_JAR_OPTION +" to add to mapper")
				.build();		
		options.addOption(functionsOption);
		
		Option outputOption =
				Option.builder(OUTPUT_FILE_OPTION)
				.longOpt(OUTPUT_FILE_OPTION_LONG)
				.argName(OUTPUT_FILE_OPTION_LONG)
				.hasArg()
				.required()
				.desc("Output file (absolute path)")
				.build();		
		options.addOption(outputOption);
		
		Option outputFormatOption =
				Option.builder(OUTPUT_FORMAT_OPTION)
				.longOpt(OUTPUT_FORMAT_OPTION_LONG)
				.argName(OUTPUT_FORMAT_OPTION_LONG)
				.hasArg()
				.desc("Output RDF format (see -"+ MAPPING_FORMAT_OPTION +")")
				.build();		
		options.addOption(outputFormatOption);
		
		Option outputNamespaceOption =
				Option.builder(OUTPUT_NAMESPACE_OPTION)
				.longOpt(OUTPUT_NAMESPACE_OPTION_LONG)
				.argName(OUTPUT_NAMESPACE_OPTION_LONG)
				.hasArgs()
				.desc("Select namespaces to prefix from provided context file, "
						+ "or from standard prefix.cc context https://prefix.cc/context")
				.build();		
		options.addOption(outputNamespaceOption);
		
		Option outputContextOption =
				Option.builder(OUTPUT_CONTEXT_OPTION)
				.longOpt(OUTPUT_CONTEXT_OPTION_LONG)
				.argName(OUTPUT_CONTEXT_OPTION_LONG)
				.hasArg()
				.desc("JSON-LD Context file containing namespace prefix declarations")
				.build();		
		options.addOption(outputContextOption);
	}
	
	private static void writeToFile(Model model, String outputFilePath, RDFFormat format) {
		FileWriter fw;
		
		try {
			fw = new FileWriter(outputFilePath);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		BufferedWriter writer = new BufferedWriter(fw);
		WriterConfig config = new WriterConfig();
		config.set(BasicWriterSettings.PRETTY_PRINT, true);
		Rio.write(model, writer, format, config);
	}
	
}
