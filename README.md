# sparql-to-flink

An approach for transforming a given SPARQL query into an Apache Flink 1.7.2 Program for querying massive static RDF data. 

## Compile the Sparkql2Flink artifact

Deploy with maven usign the configuration in pom.xml

```
mvn clean install compile package
```

## Transform your SPARQL query to an Apache Flink Program

Run the Sparql2FLink java library with the name of the query file and the name of the input dataset

```
java -cp target/sparql-to-flink-1.0-SNAPSHOT.jar org.univalle.rdf.Sparql2FLink examples/query.rq
```

## Create the Flink Program .jar to be runned on your Flink cluster

Deploy with maven using the configuration in pom_sparql2flink_jar.xml

```
mvn -f pom_sparql2flink_jar.xml clean package
```