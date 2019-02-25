package sparql2flink.out;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import sparql2flink.runner.functions.*;
import sparql2flink.runner.LoadTransformTriples;
import sparql2flink.runner.functions.order.*;
import java.math.*;

public class Query {
	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		if (!params.has("dataset") && !params.has("output")) {
			System.out.println("Use --dataset to specify dataset path and use --output to specify output path.");
		}

		//************ Environment (DataSet) and Source (static RDF dataset) ************
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Triple> dataset = LoadTransformTriples.loadTriplesFromDataset(env, params.get("dataset"));

		//************ Applying Transformations ************
		DataSet<SolutionMapping> sm1 = dataset
			.filter(new T2T_FF("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature107", null, null))
			.map(new T2SM_MF(null, "?property", "?hasValue"));

		DataSet<SolutionMapping> sm2 = dataset
			.filter(new T2T_FF(null, null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature107"))
			.map(new T2SM_MF("?isValueOf", "?property", null));

		DataSet<SolutionMapping> sm3 = sm1.union(sm2);

		DataSet<SolutionMapping> sm4 = sm3
			.map(new SM2SM_PF(new String[]{"?property", "?hasValue", "?isValueOf"}));

		DataSet<SolutionMapping> sm5 = sm4
			.distinct(new SM_DKS());

		//************ Sink  ************
		sm5.writeAsText(params.get("output")+"Query-Flink-Result", FileSystem.WriteMode.OVERWRITE)
			.setParallelism(1);

		env.execute("SPARQL Query to Flink Programan - DataSet API");
	}
}