# SPARQL2Flink library

An approach for transforming a given SPARQL query into an Apache Flink 1.7.2 program for querying massive static RDF data. 

## System requirements

* Oracle JDK 1.8.0_144
* Apache Maven 3.5.0 or higher

## Compile the SPARQL2Flink artifact

Deploy with maven usign the configuration in pom.xml

```
mvn clean install compile package
```

## Transform your SPARQL query to an Apache Flink program

Run the sparql2fLink java library with the name of the query file and the name of the input dataset

```
java -cp target/sparql2flink-1.0-SNAPSHOT.jar SPARQL2FLink examples/query.rq
```

## Create the Flink program as .jar file to be runned on your Flink local cluster

Deploy with maven using the configuration in pom_query.xml

```
mvn -f pom_query.xml clean package
```
