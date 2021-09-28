# SPARQL2Flink library

According to Apache Flink [Carbone et al., 2015], a Flink program usually consists on four basic stages: i) loading/creating the initial data, ii) specifying the transformations of the data, iii) specifying where to put the results of the computations, and iv) triggering the program execution. The SPARQL2Flink library –available on Github under the MIT license, is focused on the first three stages of a Flink program, and it is composed of two modules, called: Mapper and Runner, as shown in Figure 1. 

![Image text](/examples/sparql2flink.png)

The Mapper module allows the transformation of a SPARQL query to a DataSet Flink program. The Runner module enables the execution of a Flink program (previously compiled and packaged as a .jar file) and solves the SPARQL query. A particularity of the Java library is that it allows to transformation and process of SPARQL queries of type start and path (or chain). Apache Jena ARQ and Apache Flink libraries are shared among both modules. 

## Mapper module
The Mapper module transforms a declarative SPARQL query into an Apache Flink program. This module is based on the mapping to translate SPARQL queries to PACT transformations described in Section 4.2, and it is composed by three submodules: i) Load SPARQL Query File, ii) Translate Query to a Logical Query Plan, and iii) Convert Logical Query Plan into Flink program, described as follows:

### Load SPARQL Query File
This submodule loads the declarative SPARQL query from a file with a .rq extension. Listing 6.1 shows an example of a SPARQL query that retrieves the names of all the people with their email if they have it.
```
PREFIX foaf : <http://xmlns.com/foaf/0.1/>

SELECT ?person ?name ?mbox
WHERE {
  ?person foaf :name ?name .
  OPTIONAL { ?person foaf :mbox ?mbox }
}
```

### Translate Query to a Logical Query Plan
1 2 3 4
This submodule uses the Jena ARQ library to translate the SPARQL query into a Logical Query Plan (LQP) expressed with SPARQL Algebra operators. The LQP is represented with an RDF-centric syntax provided by Jena, which is called SPARQL Syntax Expression (SSE) [Apache-Jena, 2011]. Listing 6.2 shows an LQP of the SPARQL query example.
```
(project (? person ?name ?mbox)
  (leftjoin
    (bgp ( triple ?person <http://xmlns.com/foaf/0.1/name> ?name))
    (bgp ( triple ?person <http://xmlns.com/foaf/0.1/mbox> ?mbox))
  )
 )
```

### Convert Logical Query Plan into DataSet Flink program
This submodule converts each SPARQL Algebra operator into the query to a DataSet API transformation in Apache Flink. The Java Flink program corresponding to the SPARQL query example represented in the above Logical Query Plan under the SPARQL Syntax Expression (SSE) is shown as follows:

```
...
public class Query {
  public static void main( String [] a) throws Exception {
    /*** Environment and Source ( static RDF dataset ) ***/
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    DataSet<Triple> dataset = LoadTriples.fromDataset(env, params.get("dataset"));
    
    /*** Applying DataSet API Transformations ***/
    DataSet<SolutionMapping> sm1 = dataset
      .filter(new Triple2Triple(null, "http://xmlns.com/foaf/0.1/name", null))
      .map(new Triple2SolutionMapping ("?person", null, "?name"));
    
    DataSet < SolutionMapping > sm2 = dataset
      .filter(new Triple2Triple(null, "http://xmlns.com/foaf/0.1/mbox", null))
      .map(new Triple2SolutionMapping ("?person" , null , "?mbox"));
    
    DataSet<SolutionMapping> sm3 = sm1.leftOuterJoin(sm2)
      .where(new JoinKeySelector(new String[]{"?person"}))
      .equalTo(new JoinKeySelector(new String[]{"?person"}))
      . with (new LeftJoin (new String []{"?person"}));
      
    DataSet <SolutionMapping> sm4 = sm3
      .map(new Project (new String []{"?person", "?name", "?mbox"}));
    
    /*** Sink ***/
    sm4.writeAsText(param.get("output")+"Result", FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1);
    
    env.execute("SPARQL Query to Flink program");
  }
}
```

## Runner module
This module allows executing a Flink program (as a jar file) on an Apache Flink stand-alone or local cluster mode. This module is composed of two submodules: i) Load static RDF dataset that loads an static RDF dataset in N-Triples format and ii) Functions that contains several Java classes that allows to solve the transformations within the Flink program.


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

## References
Carbone, P., Katsifodimos, A., Ewen, S., Markl, V., Haridi, S., and Tzoumas, K. (2015). Apache flink: Stream and batch processing in a single engine. IEEE Data Eng. Bull., 38(4):28–38

Apache-Jena (2011). Sparql syntax expression. (https://jena.apache.org/documentation/notes/sse.html). Apache Software Fundation. [Online; accessed November 21, 2017]. 
