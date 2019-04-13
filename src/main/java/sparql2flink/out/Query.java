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
			.filter(new T2T_FF(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/product", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product13"))
			.map(new T2SM_MF("?offer", null, null));

		DataSet<SolutionMapping> sm2 = dataset
			.filter(new T2T_FF(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/vendor", null))
			.map(new T2SM_MF("?offer", null, "?vendor"));

		DataSet<SolutionMapping> sm3 = sm1.join(sm2)
			.where(new SM_JKS(new String[]{"?offer"}))
			.equalTo(new SM_JKS(new String[]{"?offer"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm4 = dataset
			.filter(new T2T_FF(null, "http://purl.org/dc/elements/1.1/publisher", null))
			.map(new T2SM_MF("?offer", null, "?vendor"));

		DataSet<SolutionMapping> sm5 = sm3.join(sm4)
			.where(new SM_JKS(new String[]{"?offer", "?vendor"}))
			.equalTo(new SM_JKS(new String[]{"?offer", "?vendor"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm6 = dataset
			.filter(new T2T_FF(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/country", "http://downlode.org/rdf/iso-3166/countries#GB"))
			.map(new T2SM_MF("?vendor", null, null));

		DataSet<SolutionMapping> sm7 = sm5.join(sm6)
			.where(new SM_JKS(new String[]{"?vendor"}))
			.equalTo(new SM_JKS(new String[]{"?vendor"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm8 = dataset
			.filter(new T2T_FF(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/deliveryDays", null))
			.map(new T2SM_MF("?offer", null, "?deliveryDays"));

		DataSet<SolutionMapping> sm9 = sm7.join(sm8)
			.where(new SM_JKS(new String[]{"?offer"}))
			.equalTo(new SM_JKS(new String[]{"?offer"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm10 = dataset
			.filter(new T2T_FF(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/price", null))
			.map(new T2SM_MF("?offer", null, "?price"));

		DataSet<SolutionMapping> sm11 = sm9.join(sm10)
			.where(new SM_JKS(new String[]{"?offer"}))
			.equalTo(new SM_JKS(new String[]{"?offer"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm12 = dataset
			.filter(new T2T_FF(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/validTo", null))
			.map(new T2SM_MF("?offer", null, "?date"));

		DataSet<SolutionMapping> sm13 = sm11.join(sm12)
			.where(new SM_JKS(new String[]{"?offer"}))
			.equalTo(new SM_JKS(new String[]{"?offer"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm14 = sm13
			.filter(new SM2SM_FF("(<= ?deliveryDays 3)"));

		DataSet<SolutionMapping> sm15 = sm14
			.filter(new SM2SM_FF("(> ?date \"2000-01-01T00:00:00\")"));

		DataSet<SolutionMapping> sm16 = sm15
			.map(new SM2SM_PF(new String[]{"?offer", "?price"}));

		DataSet<SolutionMapping> sm17 = sm16
			.distinct(new SM_DKS());

		DataSet<SolutionMapping> sm18 = sm17
			.first(10);

		//************ Sink  ************
		sm18.writeAsText(params.get("output")+"Query-Flink-Result", FileSystem.WriteMode.OVERWRITE)
			.setParallelism(1);

		env.execute("SPARQL Query to Flink Programan - DataSet API");
	}
}