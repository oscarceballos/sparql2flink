package sparql2flink.runner;

import com.amazonaws.util.StringInputStream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.*;

import java.io.UnsupportedEncodingException;

import static org.apache.jena.riot.RDFLanguages.strLangNTriples;

public class String2Triple implements MapFunction<String, Triple> {

    @Override
    public Triple map(String line){
        Model inputModel = ModelFactory.createDefaultModel();
        try {
            // Load model with arg string (expecting n-triples)
            inputModel = inputModel.read(new StringInputStream(line), null, strLangNTriples);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        Statement stmt = inputModel.listStatements().nextStatement();
        return stmt.asTriple();
    }
}
