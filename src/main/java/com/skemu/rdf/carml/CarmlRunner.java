package com.skemu.rdf.carml;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.engine.RmlMapper;
import com.taxonic.carml.engine.rdf.RdfRmlMapper;
import com.taxonic.carml.logicalsourceresolver.CsvResolver;
import com.taxonic.carml.logicalsourceresolver.JsonPathResolver;
import com.taxonic.carml.logicalsourceresolver.XPathResolver;
import com.taxonic.carml.model.Resource;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.util.ModelSerializer;
import com.taxonic.carml.util.Models;
import com.taxonic.carml.util.RmlMappingLoader;
import com.taxonic.carml.util.RmlNamespaces;
import com.taxonic.carml.vocab.Rdf;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.RDFWriterRegistry;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.WriterConfig;
import org.eclipse.rdf4j.rio.helpers.BasicWriterSettings;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;
import reactor.core.publisher.Flux;

@Slf4j
@Component
@Order
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

  private static final Set<RDFFormat> POTENTIALLY_STREAMING =
      Set.of(RDFFormat.NTRIPLES, RDFFormat.NQUADS, RDFFormat.TURTLE, RDFFormat.TRIG);

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

    RmlMapper<Statement> mapper = prepareMapper(cmd);
    Set<TriplesMap> mapping = loadMapping(cmd);

    if (log.isDebugEnabled()) {
      var mappingModel = mapping.stream()
          .map(Resource::asRdf)
          .flatMap(Model::stream)
          .collect(ModelCollector.toModel());

      RmlNamespaces.applyRmlNameSpaces(mappingModel);
      getOutputNamespaceDeclarations(cmd).forEach(mappingModel::setNamespace);

      log.debug("The following mapping constructs were detected:");
      log.debug("{}",
          ModelSerializer.serializeAsRdf(mappingModel, RDFFormat.TURTLE, ModelSerializer.SIMPLE_WRITER_CONFIG, n -> n));

    }

    log.info("Executing mapping ...");
    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    Flux<Statement> statements;
    if (cmd.hasOption(INPUT_FILE_OPTION)) {
      var inputPath = Paths.get(cmd.getOptionValue(INPUT_FILE_OPTION));
      try {
        var inputStream = Files.newInputStream(inputPath);
        statements = mapper.map(inputStream);
      } catch (IOException exception) {
        throw new CarmlJarException(String.format("Could not read input file %s", inputPath), exception);
      }
    } else {
      statements = mapper.map();
    }

    handleOutput(cmd, statements);
    stopWatch.stop();
    log.info("Finished processing.");
    log.info("Processing took: {} seconds,{}{}",stopWatch.getTotalTimeSeconds(), System.lineSeparator(), stopWatch.prettyPrint());

    // TODO cleanup resources / close inputstream
  }


  private void handleOutput(CommandLine cmd, Flux<Statement> statements) {
    String outputPath = cmd.getOptionValue(OUTPUT_FILE_OPTION);

    if (outputPath == null) {
      log.info("No output file specified. Outputting to console...{}", System.lineSeparator());
      writeRdf(statements, loadOutputRdfFormat(cmd), getOutputNamespaceDeclarations(cmd), false, System.out);
    } else {
      log.info("Writing output to {} ...", outputPath);
      try (var outputStream = new BufferedOutputStream(Files
          .newOutputStream(Paths.get(outputPath), StandardOpenOption.CREATE))) {
        writeRdf(statements, loadOutputRdfFormat(cmd), getOutputNamespaceDeclarations(cmd), false, outputStream);
      } catch (IOException ioException) {
        ioException.printStackTrace();
      }
    }
  }

  private void help() {
    helpFormatter.printHelp("carml", options, true);
  }

  private RmlMapper<Statement> prepareMapper(CommandLine cmd) {
    Objects.requireNonNull(cmd);

    var mapperBuilder = RdfRmlMapper.builder()
        .setLogicalSourceResolver(Rdf.Ql.Csv, CsvResolver::getInstance)
        .setLogicalSourceResolver(Rdf.Ql.JsonPath, JsonPathResolver::getInstance)
        .setLogicalSourceResolver(Rdf.Ql.XPath, XPathResolver::getInstance)
        .triplesMaps(loadMapping(cmd));

    if (cmd.hasOption(FUNCTION_JAR_OPTION) && cmd.hasOption(FUNCTION_OPTION)) {
      Set<String> fnClasses = ImmutableSet.copyOf(cmd.getOptionValues(FUNCTION_OPTION));

      Set<File> fnJars = Arrays.stream(cmd.getOptionValues(FUNCTION_JAR_OPTION))
          .map(File::new)
          .collect(ImmutableSet.toImmutableSet());

      log.debug("Loading transformation functions ...");
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

    String[] pathsToFile = cmd.getOptionValues(MAPPING_FILE_OPTION);
    Path[] paths = Arrays.stream(pathsToFile)
        .map(Paths::get)
        .toArray(Path[]::new);

    log.info("Loading mapping from {} ...", Arrays.asList(paths));

    if (cmd.hasOption(MAPPING_FORMAT_OPTION)) {
      String format = cmd.getOptionValue(MAPPING_FORMAT_OPTION);
      var rdfFormat = determineRdfFormat(format).orElseThrow(
          () -> new MappingException(String.format("Unrecognized mapping format '%s' specified.", format)));

      return RmlMappingLoader.build().load(rdfFormat, paths);
    } else {
      Model mappingModel = Arrays.stream(paths)
          .flatMap(path -> resolvePaths(path).stream())
          .flatMap(path -> {
            try (var is = Files.newInputStream(path)) {
              var fileName = path.getFileName().toString();
              Optional<RDFFormat> rdfFormat = Rio.getParserFormatForFileName(path.getFileName().toString());

              var model = rdfFormat.map(pathFormat -> Models.parse(is, pathFormat)).orElseGet(null);
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
      var contextFile = new File(cmd.getOptionValue(OUTPUT_CONTEXT_OPTION));
      namespaces.addAll(ContextLoader.getNamespaces(contextFile));
    }

    return ImmutableSet.copyOf(namespaces);
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    options = new Options();
    cmdParser = new DefaultParser();
    helpFormatter = new HelpFormatter();

    var mappingOption =
        Option.builder(MAPPING_FILE_OPTION)
            .longOpt(MAPPING_FILE_OPTION_LONG)
            .argName(MAPPING_FILE_OPTION_LONG)
            .hasArgs()
            .required()
            .desc("Mapping file path(s) and/or mapping file directory path(s)")
            .build();
    options.addOption(mappingOption);

    var formatOption =
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

    var relSrcLocOption =
        Option.builder(RELATIVE_SOURCE_LOCATION_OPTION)
            .longOpt(RELATIVE_SOURCE_LOCATION_OPTION_LONG)
            .argName(RELATIVE_SOURCE_LOCATION_OPTION_LONG)
            .hasArg()
            .desc("Specify directory to use to find relative logical source in mapping file")
            .build();
    options.addOption(relSrcLocOption);

    var inputOption =
        Option.builder(INPUT_FILE_OPTION)
            .longOpt(INPUT_FILE_OPTION_LONG)
            .argName(INPUT_FILE_OPTION_LONG)
            .hasArg()
            .desc("Input file path. For dynamic binding of input source. If provided, will be bound as the " +
                "`rml:source` for logical sources. This requires usage of `carl:Stream`")
            .build();
    options.addOption(inputOption);

    var functionJarsOption =
        Option.builder(FUNCTION_JAR_OPTION)
            .longOpt(FUNCTION_JAR_OPTION_LONG)
            .argName(FUNCTION_JAR_OPTION_LONG)
            .hasArgs()
            .desc("Jar files containing transformation functions to add to mapper")
            .build();
    options.addOption(functionJarsOption);

    var functionsOption =
        Option.builder(FUNCTION_OPTION)
            .longOpt(FUNCTION_OPTION_LONG)
            .argName(FUNCTION_OPTION_LONG)
            .hasArgs()
            .desc(String.format("Transformation function classes from -%s to add to mapper", FUNCTION_JAR_OPTION))
            .build();
    options.addOption(functionsOption);

    var outputOption =
        Option.builder(OUTPUT_FILE_OPTION)
            .longOpt(OUTPUT_FILE_OPTION_LONG)
            .argName(OUTPUT_FILE_OPTION_LONG)
            .hasArg()
            .desc("Output file path, if left empty will output to console")
            .build();
    options.addOption(outputOption);

    var outputFormatOption =
        Option.builder(OUTPUT_FORMAT_OPTION)
            .longOpt(OUTPUT_FORMAT_OPTION_LONG)
            .argName(OUTPUT_FORMAT_OPTION_LONG)
            .hasArg()
            .desc(String.format("Output RDF format (see -%s)", MAPPING_FORMAT_OPTION))
            .build();
    options.addOption(outputFormatOption);

    var outputNamespaceOption =
        Option.builder(OUTPUT_NAMESPACE_OPTION)
            .longOpt(OUTPUT_NAMESPACE_OPTION_LONG)
            .argName(OUTPUT_NAMESPACE_OPTION_LONG)
            .hasArgs()
            .desc("Select namespaces to prefix from provided namespace JSON-LD context file. " +
                "(see `-c`)<br>If left empty will default to [prefix.cc](https://prefix.cc) context " +
                "https://prefix.cc/context for available prefixes")
            .build();
    options.addOption(outputNamespaceOption);

    var outputContextOption =
        Option.builder(OUTPUT_CONTEXT_OPTION)
            .longOpt(OUTPUT_CONTEXT_OPTION_LONG)
            .argName(OUTPUT_CONTEXT_OPTION_LONG)
            .hasArg()
            .desc("JSON-LD Context file containing namespace prefix declarations")
            .build();
    options.addOption(outputContextOption);
  }

  private static void writeRdf(Flux<Statement> statements, RDFFormat format, Set<Namespace> namespaces, boolean pretty,
                               OutputStream outputStream) {
    if (isOutputStreamable(format, pretty)) {
      writeRdfStreamable(statements, format, namespaces, outputStream);
    } else {
      writeRdfPretty(statements, format, namespaces, outputStream);
    }
  }

  private static boolean isOutputStreamable(RDFFormat format, boolean pretty) {
    // TODO handle pretty
    return POTENTIALLY_STREAMING.contains(format);
  }

  private static void writeRdfPretty(Flux<Statement> statementFlux, RDFFormat format, Set<Namespace> namespaces,
                                     OutputStream outputStream) {
    var config = new WriterConfig();
    config.set(BasicWriterSettings.PRETTY_PRINT, true);
    Model model = statementFlux.collect(ModelCollector.toModel()).block();
    namespaces.forEach(model::setNamespace);
    Rio.write(model, outputStream, format, config);
  }

  private static void writeRdfStreamable(Flux<Statement> statementFlux, RDFFormat format, Set<Namespace> namespaces,
                                         OutputStream outputStream) {
    RDFWriter rdfWriter = Rio.createWriter(format, outputStream);

    try {
      rdfWriter.startRDF();
      namespaces.forEach(namespace -> rdfWriter.handleNamespace(namespace.getPrefix(), namespace.getName()));
      statementFlux/*.take(1000000)*/
          .doOnNext(rdfWriter::handleStatement)
          .blockLast();
      rdfWriter.endRDF();
    } catch (RDFHandlerException rdfHandlerException) {
      throw new CarmlJarException("Exception occurred while writing output.", rdfHandlerException);
    }
  }

}
