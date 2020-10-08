package com.skemu.rdf.carml;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;

public class ContextLoader {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ContextLoader() {
  }

  public static Set<Namespace> getNamespaces(String... selectedPrefixes) {
    return getNamespaces(null, selectedPrefixes);
  }

  public static Set<Namespace> getNamespaces(File file, String... selectedPrefixes) {
    Supplier<JsonNode> contextSupplier;
    if (file == null) {
      contextSupplier = () -> {
        try {
          return MAPPER.readTree(ContextLoader.class.getResourceAsStream("prefix.cc.context.ld.json"));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      };
    } else {
      contextSupplier = () -> {
        try {
          return MAPPER.readTree(file);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      };
    }

    Map<String, String> contextNamespaces = loadPrefixesFromJsonLdContext(contextSupplier);
    Set<Namespace> namespaces = contextNamespaces.entrySet().stream()
        .map(e -> new SimpleNamespace(e.getKey(), e.getValue()))
        .collect(Collectors.toSet());

    List<String> prefixesToFilter = Arrays.asList(selectedPrefixes);
    if (prefixesToFilter.isEmpty()) {
      return namespaces;
    } else {
      return namespaces.stream()
          .filter(n -> prefixesToFilter.contains(n.getPrefix()))
          .collect(Collectors.toSet());
    }
  }

  static Map<String, String> loadPrefixesFromJsonLdContext(Supplier<JsonNode> contextSupplier) {
    JsonNode node = contextSupplier.get();
    JsonNode context = node.get("@context");
    return MAPPER.convertValue(context, new TypeReference<>() {
    });
  }
}
