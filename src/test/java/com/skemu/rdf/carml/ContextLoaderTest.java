package com.skemu.rdf.carml;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.SKOS;
import org.junit.Test;

public class ContextLoaderTest {
	
	@Test
	public void getNamespaces_withNoNamespaceFilter_returnsAllNamespaces() {
		Set<Namespace> namespaces = ContextLoader.getNamespaces();
		assertThat(
				namespaces.size(), 
				is(ContextLoader.loadPrefixesFromJsonLdContext(()->  {
					try {
						return new ObjectMapper().readTree(ContextLoader.class.getResourceAsStream("prefix.cc.context.ld.json"));
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}).size()));
	}
	
	@Test
	public void getNamespaces_withNamespaceFilter_returnsSelectedNamespaces() {
		Map<String, Namespace> namespaceByName = 
				ImmutableMap.of(
						RDF.NAMESPACE, RDF.NS,
						RDFS.NAMESPACE, RDFS.NS,
						SKOS.NAMESPACE, SKOS.NS
				);
		String[] selectedPrefixes = new String[] {RDF.PREFIX, RDFS.PREFIX, SKOS.PREFIX};
		Set<Namespace> namespaces = ContextLoader.getNamespaces(selectedPrefixes);
		assertThat(namespaces.size(), is(3));
		namespaces.forEach(n -> assertThat(n, is(namespaceByName.get(n.getName()))));
	}
	
}
