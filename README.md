CARML JAR
=====================
Simple CLI tool for [CARML](https://github.com/carml/carml)

Usage:
------
| Option            | Description                      |
|-------------------|----------------------------------|
|-c,--context       |JSON-LD Context file containing namespace prefix declarations|
|-f,--format        |Mapping file RDF format.<br>If left empty the format will be determined automatically for each mapping file.<br>If provided, all mappings will be assumed to have the provided format.<br><br>The following formats are supported:<br>  ttl (text/turtle),<br>nt (application/n-triples),<br>nq (application/n-quads),<br>rdf (application/rdf+xml),<br>jsonld (application/ld+json),<br>trig (application/trig),<br>n3 (text/n3),<br>trix (application/trix),<br>brf (application/x-binary-rdf),<br>rj (application/rdf+json)|
|-fn,--functions    |Transformation function classes from `-j` to add to mapper|
|-i,--input         |Input file path. For dynamic binding of input source. If provided, will be bound as the `rml:source` for logical sources. This requires usage of `carl:Stream`|
|-j,--jars          |Jar files containing transformation functions to add to mapper|
|-m,--mapping       |Mapping file path(s) and/or mapping file directory path(s)|
|-o,--output        |Output file path, if left empty will output to console|
|-of,--outformat    |Output RDF format (see `-f`)|
|-p,--prefix        |Select namespaces to prefix from provided namespace JSON-LD context file. (see `-c`)<br>If left empty will default to [prefix.cc](https://prefix.cc) context https://prefix.cc/context for available prefixes|
|-rsl,--rel-src-loc |Specify directory to use to find relative logical source in mapping file|

```shell script
java -jar carml-jar-X.X.X.jar \
  -m some.rml.ttl \
  -rsl /path/to/source/dir \
  -j /path/to/functions/jar/functions.jar \
  -fn com.example.function.RmlFunctions \
  -o /path/to/output.ttl \
  -of ttl \
  -c /path/to/custom/jsonld/namespaces.jsonld \
  -p ex foo bar
```

`namespaces.jsonld`
```
{
    "@context": {
        "ex": "http://example.com/ex#",
        "foo": "http://example.com/foo#",
        "bar": "http://example.com/bar#"
    }
}
```

Optionally you can enable debug, or trace logging by adding one of the following options respectively
```
--spring.profiles.active=debug
--spring.profiles.active=trace
```


Building a new jar:
-------------------

```shell script
mvn clean package
```
