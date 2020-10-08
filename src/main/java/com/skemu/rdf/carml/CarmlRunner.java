package com.skemu.rdf.carml;

import com.taxonic.carml.model.Resource;
import com.taxonic.carml.util.IoUtils;
import com.taxonic.carml.util.ModelSerializer;
import com.taxonic.carml.util.RmlNamespaces;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
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

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.engine.RmlMapper;
import com.taxonic.carml.logical_source_resolver.CsvResolver;
import com.taxonic.carml.logical_source_resolver.JsonPathResolver;
import com.taxonic.carml.logical_source_resolver.XPathResolver;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.rdf_mapper.util.ImmutableCollectors;
import com.taxonic.carml.util.RmlMappingLoader;
import com.taxonic.carml.vocab.Rdf;

@Slf4j
@Component
@Order(value = Ordered.LOWEST_PRECEDENCE)
public class CarmlRunner implements CommandLineRunner, InitializingBean {

  private static final String MAPPING_FILE_OPTION = "m";

  private static final String MAPPING_FILE_OPTION_LONG = "mapping";

  private static final String MAPPING_FORMAT_OPTION = "f";

  private static final String MAPPING_FORMAT_OPTION_LONG = "format";

  private static final String RELATIVE_SOURCE_LOCATION_OPTION = "rsl";

  private static final String RELATIVE_SOURCE_LOCATION_OPTION_LONG = "rel-src-loc";

  private static final String INPUT_FILE_OPTION = "i";

  private static final String INPUT_FILE_OPTION_LONG = "input";

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
    } catch (ParseException e) {
      help();
      System.exit(1);
    }

    RmlMapper mapper = prepareMapper(cmd);
    Set<TriplesMap> mapping = loadMapping(cmd);

    if (log.isDebugEnabled()) {
      Model mappingModel = mapping.stream()
          .map(Resource::asRdf)
          .flatMap(Model::stream)
          .collect(Collectors.toCollection(LinkedHashModel::new));

      RmlNamespaces.applyRmlNameSpaces(mappingModel);
      getOutputNamespaceDeclarations(cmd).forEach(mappingModel::setNamespace);

      log.debug("The following mapping constructs were detected:");
      log.debug("{}",
          ModelSerializer.serializeAsRdf(mappingModel, RDFFormat.TURTLE, ModelSerializer.SIMPLE_WRITER_CONFIG, n -> n));

    }

    log.info("Executing mapping ...");
    Model model = mapper.map(mapping);

    getOutputNamespaceDeclarations(cmd).forEach(model::setNamespace);

    String outputPath = cmd.getOptionValue(OUTPUT_FILE_OPTION);
    if (outputPath == null) {
      log.info("No output file specified. Outputting to console...");
      StringWriter writer = new StringWriter();
      writeRdf(model, loadOutputRdfFormat(cmd), writer);

      System.out.println(String.format("%n%s", writer.toString()));
    } else {
      log.info("Writing output to {} ...", outputPath);
      writeToFile(model, outputPath, loadOutputRdfFormat(cmd));
    }
  }

  private void help() {
    helpFormatter.printHelp("carml", options, true);
  }

  private RmlMapper prepareMapper(CommandLine cmd) {
    Objects.requireNonNull(cmd);

    RmlMapper.Builder mapperBuilder = RmlMapper.newBuilder()
        .setLogicalSourceResolver(Rdf.Ql.JsonPath, new JsonPathResolver())
        .setLogicalSourceResolver(Rdf.Ql.XPath, new XPathResolver())
        .setLogicalSourceResolver(Rdf.Ql.Csv, new CsvResolver());

    if (cmd.hasOption(FUNCTION_JAR_OPTION) && cmd.hasOption(FUNCTION_OPTION)) {
      Set<String> fnClasses = ImmutableSet.copyOf(cmd.getOptionValues(FUNCTION_OPTION));

      Set<File> fnJars = Arrays.stream(cmd.getOptionValues(FUNCTION_JAR_OPTION))
          .map(File::new)
          .collect(ImmutableCollectors.toImmutableSet());

      log.debug("Loading transformation functions ...");
      Set<Object> functions = JarFunctionLoader.load(fnClasses, fnJars);
      functions.forEach(mapperBuilder::addFunctions);
    }

    if (cmd.hasOption(RELATIVE_SOURCE_LOCATION_OPTION)) {
      mapperBuilder.fileResolver(Paths.get(cmd.getOptionValue(RELATIVE_SOURCE_LOCATION_OPTION)));
    }

    RmlMapper mapper = mapperBuilder.build();

    if (cmd.hasOption(INPUT_FILE_OPTION)) {
      Path inputPath = Paths.get(cmd.getOptionValue(INPUT_FILE_OPTION));
      try(InputStream is = Files.newInputStream(inputPath)) {
        mapper.bindInputStream(is);
      } catch (IOException exception) {
        throw new CarmlJarException(String.format("Could not read input file %s", inputPath), exception);
      }
    }

    return mapper;
  }

  private Set<TriplesMap> loadMapping(CommandLine cmd) {
    Objects.requireNonNull(cmd);

    String[] pathsToFile = cmd.getOptionValues(MAPPING_FILE_OPTION);
    Path[] paths = Arrays.stream(pathsToFile)
        .map(Paths::get)
        .toArray(Path[]::new);

    log.info("Loading mapping from {} ...", paths);

    if (cmd.hasOption(MAPPING_FORMAT_OPTION)) {
      String format = cmd.getOptionValue(MAPPING_FORMAT_OPTION);
      RDFFormat rdfFormat = determineRdfFormat(format).orElseThrow(
          () -> new MappingFormatException(String.format("Unrecognized mapping format '%s' specified.", format)));

      return RmlMappingLoader.build().load(rdfFormat, paths);
    } else {
      Model mappingModel = Arrays.stream(paths)
          .flatMap(path -> resolvePaths(path).stream())
          .flatMap(path -> {
            try (InputStream is = Files.newInputStream(path)) {
              String fileName = path.getFileName().toString();
              Optional<RDFFormat> rdfFormat = Rio.getParserFormatForFileName(path.getFileName().toString());

              Model model = rdfFormat.map(pathFormat -> IoUtils.parse(is, pathFormat)).orElseGet(null);
              if (model == null) {
                log.warn("Could not determine mapping format for filename '{}', ignoring this file...", fileName);
                return Stream.empty();
              } else {
                return model.stream();
              }
            } catch (IOException exception) {
              throw new CarmlJarException(String.format("Could not read file %s", path), exception);
            }
          })
          .collect(Collectors.toCollection(LinkedHashModel::new));

      return RmlMappingLoader.build().load(mappingModel);
    }
  }

  private List<Path> resolvePaths(Path... paths) {
    return Arrays.stream(paths)
        .flatMap(path -> {
          System.out.println(path);
          try (Stream<Path> walk = Files.walk(path)) {
            return walk.filter(Files::isRegularFile)
                .collect(Collectors.toList())
                .stream();
          } catch (IOException exception) {
            throw new CarmlJarException(String.format("Exception occurred while reading file %s", path), exception);
          }
        })
        .collect(Collectors.toList());
  }

  private RDFFormat loadOutputRdfFormat(CommandLine cmd) {
    Objects.requireNonNull(cmd);
    if (cmd.hasOption(OUTPUT_FORMAT_OPTION)) {
      String format = cmd.getOptionValue(OUTPUT_FORMAT_OPTION);
      return determineRdfFormat(format).orElse(RDFFormat.NQUADS);
    }
    log.info("Defaulting to N-Quads format ...");
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
            .hasArgs()
            .required()
            .desc("Mapping file path(s) and/or mapping file directory path(s)")
            .build();
    options.addOption(mappingOption);

    Option formatOption =
        Option.builder(MAPPING_FORMAT_OPTION)
            .longOpt(MAPPING_FORMAT_OPTION_LONG)
            .argName(MAPPING_FORMAT_OPTION_LONG)
            .hasArgs()
            .desc(String.format(
                "Mapping file RDF format:%n" +
                    "ttl (text/turtle), %n" +
                    "nt (application/n-triples), %n" +
                    "nq (application/n-quads), %n" +
                    "rdf (application/rdf+xml), %n" +
                    "jsonld (application/ld+json), %n" +
                    "trig (application/trig), %n" +
                    "n3 (text/n3), %n" +
                    "trix (application/trix), %n" +
                    "brf (application/x-binary-rdf), %n" +
                    "rj (application/rdf+json)%n"))
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

    Option inputOption =
        Option.builder(INPUT_FILE_OPTION)
            .longOpt(INPUT_FILE_OPTION_LONG)
            .argName(INPUT_FILE_OPTION_LONG)
            .hasArg()
            .desc("Input file path. For dynamic binding of input source. If provided, will be bound as the " +
                "`rml:source` for logical sources. This requires usage of `carl:Stream`")
            .build();
    options.addOption(inputOption);

    Option functionJarsOption =
        Option.builder(FUNCTION_JAR_OPTION)
            .longOpt(FUNCTION_JAR_OPTION_LONG)
            .argName(FUNCTION_JAR_OPTION_LONG)
            .hasArgs()
            .desc("Jar files containing transformation functions to add to mapper")
            .build();
    options.addOption(functionJarsOption);

    Option functionsOption =
        Option.builder(FUNCTION_OPTION)
            .longOpt(FUNCTION_OPTION_LONG)
            .argName(FUNCTION_OPTION_LONG)
            .hasArgs()
            .desc(String.format("Transformation function classes from -%s to add to mapper", FUNCTION_JAR_OPTION))
            .build();
    options.addOption(functionsOption);

    Option outputOption =
        Option.builder(OUTPUT_FILE_OPTION)
            .longOpt(OUTPUT_FILE_OPTION_LONG)
            .argName(OUTPUT_FILE_OPTION_LONG)
            .hasArg()
            .desc("Output file path, if left empty will output to console")
            .build();
    options.addOption(outputOption);

    Option outputFormatOption =
        Option.builder(OUTPUT_FORMAT_OPTION)
            .longOpt(OUTPUT_FORMAT_OPTION_LONG)
            .argName(OUTPUT_FORMAT_OPTION_LONG)
            .hasArg()
            .desc(String.format("Output RDF format (see -%s)", MAPPING_FORMAT_OPTION))
            .build();
    options.addOption(outputFormatOption);

    Option outputNamespaceOption =
        Option.builder(OUTPUT_NAMESPACE_OPTION)
            .longOpt(OUTPUT_NAMESPACE_OPTION_LONG)
            .argName(OUTPUT_NAMESPACE_OPTION_LONG)
            .hasArgs()
            .desc("Select namespaces to prefix from provided namespace JSON-LD context file. " +
                "(see `-c`)<br>If left empty will default to [prefix.cc](https://prefix.cc) context " +
                "https://prefix.cc/context for available prefixes")
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

    writeRdf(model, format, fw);
  }

  private static void writeRdf(Model model, RDFFormat format, Writer writer) {
    BufferedWriter bw = new BufferedWriter(writer);
    WriterConfig config = new WriterConfig();
    config.set(BasicWriterSettings.PRETTY_PRINT, true);
    Rio.write(model, bw, format, config);
  }

}
