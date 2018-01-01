package com.skemu.rdf.carml;

import com.taxonic.carml.rdf_mapper.util.ImmutableCollectors;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Set;
import org.xeustechnologies.jcl.JarClassLoader;
import org.xeustechnologies.jcl.JclObjectFactory;

public class JarFunctionLoader {
	
	private JarFunctionLoader() {}
	
	public static Set<Object> load(Set<String> functionClasses, Set<File> jars) {
		JarClassLoader jcl = new JarClassLoader();
		try {
			for (File jar : jars) {
				jcl.add(new FileInputStream(jar));
			}
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Could not find jar file", e);
		}

		JclObjectFactory factory = JclObjectFactory.getInstance();
		return functionClasses.stream()
				.map(f -> factory.create(jcl, f))
				.collect(ImmutableCollectors.toImmutableSet());
	}

}
