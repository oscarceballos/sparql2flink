package sparql2flink.out;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import sparql2flink.runner.functions.*;
import sparql2flink.runner.LoadTriples;
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
		DataSet<Triple> dataset = LoadTriples.fromDataset(env, params.get("dataset"));

		//************ Applying Transformations ************
		DataSet<SolutionMapping> sm1 = dataset
			.filter(new Triple2Triple(null, "http://www.w3.org/2000/01/rdf-schema#label", null))
			.map(new Triple2SolutionMapping("?product", null, "?label"));

		DataSet<SolutionMapping> sm2 = dataset
			.filter(new Triple2Triple(null, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType11"))
			.map(new Triple2SolutionMapping("?product", null, null));

		DataSet<SolutionMapping> sm3 = sm1.join(sm2)
			.where(new JoinKeySelector(new String[]{"?product"}))
			.equalTo(new JoinKeySelector(new String[]{"?product"}))
			.with(new Join());

		DataSet<SolutionMapping> sm4 = dataset
			.filter(new Triple2Triple(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature40"))
			.map(new Triple2SolutionMapping("?product", null, null));

		DataSet<SolutionMapping> sm5 = sm3.join(sm4)
			.where(new JoinKeySelector(new String[]{"?product"}))
			.equalTo(new JoinKeySelector(new String[]{"?product"}))
			.with(new Join());

		DataSet<SolutionMapping> sm6 = dataset
			.filter(new Triple2Triple(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature417"))
			.map(new Triple2SolutionMapping("?product", null, null));

		DataSet<SolutionMapping> sm7 = sm5.join(sm6)
			.where(new JoinKeySelector(new String[]{"?product"}))
			.equalTo(new JoinKeySelector(new String[]{"?product"}))
			.with(new Join());

		DataSet<SolutionMapping> sm8 = dataset
			.filter(new Triple2Triple(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1", null))
			.map(new Triple2SolutionMapping("?product", null, "?value1"));

		DataSet<SolutionMapping> sm9 = sm7.join(sm8)
			.where(new JoinKeySelector(new String[]{"?product"}))
			.equalTo(new JoinKeySelector(new String[]{"?product"}))
			.with(new Join());

		DataSet<SolutionMapping> sm10 = sm9
			.filter(new Filter("(> ?value1 10)"));

		DataSet<SolutionMapping> sm11 = sm10
			.map(new Project(new String[]{"?product", "?label"}));

		DataSet<SolutionMapping> sm12 = sm11
			.distinct(new DistinctKeySelector());

		DataSet<SolutionMapping> sm13 = sm12
					.sortPartition(new OrderKeySelector("?label"), Order.ASCENDING)
					.setParallelism(1);
	
		DataSet<SolutionMapping> sm14 = sm13
			.first(10);

		//************ Sink  ************
		sm14.writeAsText(params.get("output")+"Query-Flink-Result", FileSystem.WriteMode.OVERWRITE)
			.setParallelism(1);

		env.execute("SPARQL Query to Flink Programan - DataSet API");
	}
}