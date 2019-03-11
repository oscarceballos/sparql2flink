package sparql2flink.out;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.jena.graph.Triple;
import sparql2flink.runner.functions.*;
import sparql2flink.runner.LoadTransformTriples;
import sparql2flink.runner.functions.order.*;

public class Query {
	public static void main(String[] args) throws Exception {

		//************ Environment (DataSet) and Source (static RDF dataset) ************
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Triple> dataset = LoadTransformTriples.loadTriplesFromDataset(env, "./data/dataset20mb.nt");

		//************ Applying Transformations ************
		DataSet<SolutionMapping> sm1 = dataset
			.filter(new T2T_FF(null, "http://www.w3.org/2000/01/rdf-schema#label", null))
			.map(new T2SM_MF("?product", null, "?label"))
            .first(10);

        DataSet<SolutionMapping> sm2 = sm1
                .map(new SM2SM_PF(new String[]{"?label"}));

        DataSet<SolutionMapping> sm3 = sm2
                .distinct(new SM_DKS());

		DataSet<SolutionMapping> sm4 = sm3
                .sortPartition(new SM_OKS("?label"), Order.ASCENDING)
                .setParallelism(1);

		//************ Sink  ************
		sm4.writeAsText("./data/Query-Flink-Result", FileSystem.WriteMode.OVERWRITE)
			.setParallelism(1);

		env.execute("SPARQL Query to Flink Programan - DataSet API");

	}
}