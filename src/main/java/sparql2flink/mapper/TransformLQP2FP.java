package sparql2flink.mapper;

import org.apache.jena.sparql.algebra.Op;
import java.nio.file.Path;

public class TransformLQP2FP {

    private Op lqp;
    private String className;

    public TransformLQP2FP(Op lqp, Path queryFile){
        this.lqp = lqp;
        this.className = queryFile.getFileName().toString();
        this.className = this.className.substring(0, this.className.indexOf('.'));
        this.className = this.className.toLowerCase();
        this.className = this.className.substring(0, 1).toUpperCase() + this.className.substring(1, this.className.length());
    }

    public String lQP2FP() {
        String fp = "";

        fp += "package sparql2flink.out;\n\n" +
                "import org.apache.flink.api.java.DataSet;\n" +
                "import org.apache.flink.api.common.operators.Order;\n" +
                "import org.apache.flink.api.java.ExecutionEnvironment;\n" +
                "import org.apache.flink.api.java.utils.ParameterTool;\n" +
                "import org.apache.flink.core.fs.FileSystem;\n" +
                "import org.apache.jena.graph.Node;\n" +
                "import org.apache.jena.graph.Triple;\n" +
                "import sparql2flink.runner.functions.*;\n" +
                "import sparql2flink.runner.LoadTransformTriples;\n" +
                "import sparql2flink.runner.functions.order.*;\n" +
                "import java.math.*;\n" +
                "\npublic class "+className+" {\n" +
                "\tpublic static void main(String[] args) throws Exception {\n\n" +
                "\t\tfinal ParameterTool params = ParameterTool.fromArgs(args);\n\n" +
                "\t\tif (!params.has(\"dataset\") && !params.has(\"output\")) {\n" +
                "\t\t\tSystem.out.println(\"Use --dataset to specify dataset path and use --output to specify output path.\");\n" +
                "\t\t}\n\n" +
                "\t\t//************ Environment (DataSet) and Source (static RDF dataset) ************\n" +
                "\t\tfinal ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();\n" +
                "\t\tDataSet<Triple> dataset = LoadTransformTriples.loadTriplesFromDataset(env, params.get(\"dataset\"));\n\n" +
                "\t\t//************ Applying Transformations ************\n";

        lqp.visit(new ConvertLQP2FP());

        fp += ConvertLQP2FP.getFp();

        fp += "\t\t//************ Sink  ************\n" +
                "\t\tsm"+(SolutionMapping.getIndice()-1) +
                ".writeAsText(params.get(\"output\")+\""+className+"-Flink-Result\", FileSystem.WriteMode.OVERWRITE)\n" +
                "\t\t\t.setParallelism(1);\n\n" +
                "\t\tenv.execute(\"SPARQL Query to Flink Programan - DataSet API\");\n" +
                "\t}\n}";

        return fp;
    }
}
