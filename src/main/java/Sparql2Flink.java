
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.jena.sparql.algebra.Op;
import sparql2flink.mapper.CreateFPFile;
import sparql2flink.mapper.LoadSQFile;
import sparql2flink.mapper.TransformLQP2FP;
import sparql2flink.mapper.TranslationSQ2LQP;

public class Sparql2Flink {

	public static void main(String[] args) throws Exception {
	    Path queryFile=null;

        if (args != null && args.length == 1) {
            queryFile = Paths.get(args[0]);
        } else {
            System.out.println("\nYou should to specify path query file argument.\nFor example: path_query_file/query_file.rq\n"+
                    "\nExecuting sample with default SPARQL query saved in << examples >> directory");
            // get default query file
            queryFile = Paths.get("./examples/query.rq");
        }

        LoadSQFile sparqlQueryFile = new LoadSQFile(queryFile.toString());
        String queryString = sparqlQueryFile.loadSQFile();

        TranslationSQ2LQP sq2LQP = new TranslationSQ2LQP(queryString);
        Op lqp = sq2LQP.translationSQ2LQP();

        TransformLQP2FP lqp2FP = new TransformLQP2FP(lqp, queryFile);
        String fp = lqp2FP.lQP2FP();

        CreateFPFile jfp = new CreateFPFile(fp, queryFile);
        jfp.createFPFile();
	}
}
