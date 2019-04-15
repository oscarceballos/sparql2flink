package sparql2flink.mapper;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

import java.util.ArrayList;

public class ConvertTriplePattern2 {

    public ConvertTriplePattern2(){}

    public static String convertObject(Node node){
        String obj = "";
        if(node.isLiteral()) {
            if (node.getLiteralDatatype().getJavaClass().equals(String.class)){
                obj = "\"" + node.getLiteralValue().toString() + "\"";
            } else {
                obj = node.toString();
            }
        } else if (node.isURI()) {
            obj = "\"" + node.getURI().toString() + "\"";
        }
        return obj;
    }

    public static String convert(Triple triple, Integer indice){

        String flatMap = "";

        String subject = null;
        String predicate = null;
        String object = null;

        if(triple.getSubject().isVariable() && triple.getPredicate().isVariable() && triple.getObject().isVariable()) {
            subject = "\""+triple.getSubject().toString()+"\"";
            predicate = "\""+triple.getPredicate().toString()+"\"";
            object = "\""+triple.getObject().toString()+"\"";

            ArrayList<String> variables = new ArrayList<>();
            variables.add(subject);
            variables.add(predicate);
            variables.add(object);
            SolutionMapping.insertSolutionMapping(indice, variables);

        } else if(triple.getSubject().isVariable() && triple.getPredicate().isVariable() && !triple.getObject().isVariable()) {
            object = convertObject(triple.getObject());

            subject = "\""+triple.getSubject().toString()+"\"";
            predicate = "\""+triple.getPredicate().toString()+"\"";

            ArrayList<String> variables = new ArrayList<>();
            variables.add(subject);
            variables.add(predicate);
            SolutionMapping.insertSolutionMapping(indice, variables);

        } else if(triple.getSubject().isVariable() && !triple.getPredicate().isVariable() && !triple.getObject().isVariable()) {
            predicate = "\""+triple.getPredicate().toString()+"\"";
            object = convertObject(triple.getObject());

            subject = "\""+triple.getSubject().toString()+"\"";

            ArrayList<String> variables = new ArrayList<>();
            variables.add(subject);
            SolutionMapping.insertSolutionMapping(indice, variables);

        } else if(triple.getSubject().isVariable() && !triple.getPredicate().isVariable() && triple.getObject().isVariable()) {
            predicate = "\""+triple.getPredicate().toString()+"\"";

            subject = "\""+triple.getSubject().toString()+"\"";
            object = "\""+triple.getObject().toString()+"\"";

            ArrayList<String> variables = new ArrayList<>();
            variables.add(subject);
            variables.add(object);
            SolutionMapping.insertSolutionMapping(indice, variables);

        } else if(!triple.getSubject().isVariable() && triple.getPredicate().isVariable() && triple.getObject().isVariable()) {
            subject = "\""+triple.getSubject().toString()+"\"";

            predicate = "\""+triple.getPredicate().toString()+"\"";
            object = "\""+triple.getObject().toString()+"\"";

            ArrayList<String> variables = new ArrayList<>();
            variables.add(predicate);
            variables.add(object);
            SolutionMapping.insertSolutionMapping(indice, variables);

        } else if(!triple.getSubject().isVariable() && !triple.getPredicate().isVariable() && triple.getObject().isVariable()) {
            subject = "\""+triple.getSubject().toString()+"\"";
            predicate = "\""+triple.getPredicate().toString()+"\"";

            object = "\""+triple.getObject().toString()+"\"";

            ArrayList<String> variables = new ArrayList<>();
            variables.add(object);
            SolutionMapping.insertSolutionMapping(indice, variables);

        } else if(!triple.getSubject().isVariable() && !triple.getPredicate().isVariable() && !triple.getObject().isVariable()) {
            subject = "\""+triple.getSubject().toString()+"\"";
            predicate = "\""+triple.getPredicate().toString()+"\"";
            object = convertObject(triple.getObject());

            SolutionMapping.insertSolutionMapping(indice, null);
        }

        flatMap += "\t\t\t.flatMap(new Triple2SolutionMapping2("+subject+", "+predicate+", "+object+"));\n\n";

        return flatMap;
    }
}
