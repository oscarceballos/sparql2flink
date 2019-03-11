package sparql2flink.out;

import akka.io.Tcp;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import sparql2flink.runner.functions.*;
import sparql2flink.runner.LoadTransformTriples;
import sparql2flink.runner.functions.order.*;
import java.math.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
			.filter(new T2T_FF(null, "http://www.w3.org/2000/01/rdf-schema#label", null))
			.map(new T2SM_MF("?product", null, "?label"));

		DataSet<SolutionMapping> sm2 = dataset
			.filter(new T2T_FF(null, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType17"))
			.map(new T2SM_MF("?product", null, null));

		DataSet<SolutionMapping> sm3 = sm1.join(sm2)
			.where(new SM_JKS(new String[]{"?product"}))
			.equalTo(new SM_JKS(new String[]{"?product"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm4 = dataset
			.filter(new T2T_FF(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature106"))
			.map(new T2SM_MF("?product", null, null));

		DataSet<SolutionMapping> sm5 = sm3.join(sm4)
			.where(new SM_JKS(new String[]{"?product"}))
			.equalTo(new SM_JKS(new String[]{"?product"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm6 = dataset
			.filter(new T2T_FF(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature100"))
			.map(new T2SM_MF("?product", null, null));

		DataSet<SolutionMapping> sm7 = sm5.join(sm6)
			.where(new SM_JKS(new String[]{"?product"}))
			.equalTo(new SM_JKS(new String[]{"?product"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm8 = dataset
			.filter(new T2T_FF(null, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1", null))
			.map(new T2SM_MF("?product", null, "?value1"));

		DataSet<SolutionMapping> sm9 = sm7.join(sm8)
			.where(new SM_JKS(new String[]{"?product"}))
			.equalTo(new SM_JKS(new String[]{"?product"}))
			.with(new SM_JF());

		DataSet<SolutionMapping> sm10 = sm9
			.filter(new SM2SM_FF("(> ?value1 10)"));

		DataSet<SolutionMapping> sm11 = sm10
			.map(new SM2SM_PF(new String[]{"?product", "?label"}));

		DataSet<SolutionMapping> sm12 = sm11
			.distinct(new SM_DKS());

        /*final List<Node> nodeList = new ArrayList<>();

        DataSet<Node> nodes = sm12
                .first(1)
                .map(new MapFunction<SolutionMapping, Node>() {
                    @Override
                    public Node map(SolutionMapping sm) {
                        nodeList.add(sm.getValue("?label"));
                        return sm.getValue("?label");
                    }
                });

        Node node = nodeList.get(0);
        System.out.println("Node: "+node);*/

        DataSet<SolutionMapping> sm13 = sm12
                .sortPartition(new SM_OKS("?label"), Order.ASCENDING)
                .setParallelism(1);

        //sm12.collect().get(0).getValue("?label");


        /*DataSet<SolutionMapping> sm13;
		if(node.isLiteral()) {
			if(node.getLiteralValue().getClass().equals(BigDecimal.class) || node.getLiteralValue().getClass().equals(Double.class)){
				sm13 = sm12
					.sortPartition(new SM_OKS_Double("?label"), Order.ASCENDING)
					.setParallelism(1);
			} else if (node.getLiteralValue().getClass().equals(BigInteger.class) || node.getLiteralValue().getClass().equals(Integer.class)) {
				sm13 = sm12
					.sortPartition(new SM_OKS_Integer("?label"), Order.ASCENDING)
					.setParallelism(1);
			} else if (node.getLiteralValue().getClass().equals(Float.class)) {
				sm13 = sm12
					.sortPartition(new SM_OKS_Float("?label"), Order.ASCENDING)
					.setParallelism(1);
			} else if (node.getLiteralValue().getClass().equals(Long.class)){
				sm13 = sm12
					.sortPartition(new SM_OKS_Long("?label"), Order.ASCENDING)
					.setParallelism(1);
			} else {
				sm13 = sm12
					.sortPartition(new SM_OKS_String("?label"), Order.ASCENDING)
					.setParallelism(1);
			}
		} else {
				sm13 = sm12
					.sortPartition(new SM_OKS_String("?label"), Order.ASCENDING)
					.setParallelism(1);
		}*/

		DataSet<SolutionMapping> sm14 = sm13
			.first(10);

		//************ Sink  ************
		sm14.writeAsText(params.get("output")+"Query-Flink-Result", FileSystem.WriteMode.OVERWRITE)
			.setParallelism(1);

		env.execute("SPARQL Query to Flink Programan - DataSet API");


        //List<SolutionMapping> outData = new ArrayList<>();
        //sm12.output(new LocalCollectionOutputFormat(outData));

        //System.out.println(outData.get(0).toString());


	}
}