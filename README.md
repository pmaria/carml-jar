CARML JAR
=====================
Simple CLI tool for [CARML](https://github.com/carml/carml)

Usage:
------
| Option            | Description                      |
|-------------------|----------------------------------|
|-c,--context       |JSON-LD Context file containing namespace prefix declarations|
|-f,--format        |Mapping file RDF format:<br>                     ttl (text/turtle),<br>nt (application/n-triples),<br>nq (application/n-quads),<br>rdf (application/rdf+xml),<br>jsonld (application/ld+json),<br>trig (application/trig),<br>n3 (text/n3),<br>trix (application/trix),<br>brf (application/x-binary-rdf),<br>rj (application/rdf+json)|
|-fn,--functions    |Transformation function classes from -j to add to mapper|
|-j,--jars          |Jar files conatining transformation functions to add to mapper|
|-m,--mapping       |Mapping file|
|-o,--output        |Output file (absolute path)|
|-of,--outformat    |Output RDF format (see -f)|
|-p,--prefix        |Select namespaces to prefix from [prefix.cc](https://prefix.cc) context https://prefix.cc/context|
|-rsl,--rel-src-loc |Specify directory to use to find relative logical source in mapping file|

Example:

```
java -jar carml-jar-X.X.X.jar \
  -m some.rml.ttl \
  -rsl /path/to/source/dir \
  -j /path/to/functions/jar/functions.jar \
  -fn com.example.function.RmlFunctions \
  -o /path/to/output.ttl \
  -of ttl \
  -c /path/to/custom/jsonld/namespaces.jsonld \
  -p dct xsd
```
