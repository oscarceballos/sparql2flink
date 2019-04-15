package sparql2flink.mapper;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

import java.util.ArrayList;

public class ConvertTriplePattern {

    public ConvertTriplePattern(){}

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

        String filter_map = "";

        String subject_filter = null;
        String predicate_filter = null;
        String object_filter = null;

        String subject_map = null;
        String predicate_map = null;
        String object_map = null;

        if(triple.getSubject().isVariable() && triple.getPredicate().isVariable() && triple.getObject().isVariable()) {
            subject_map = "\""+triple.getSubject().toString()+"\"";
            predicate_map = "\""+triple.getPredicate().toString()+"\"";
            object_map = "\""+triple.getObject().toString()+"\"";

            ArrayList<String> variables = new ArrayList<>();
            variables.add(subject_map);
            variables.add(predicate_map);
            variables.add(object_map);
            SolutionMapping.insertSolutionMapping(indice, variables);

        } else if(triple.getSubject().isVariable() && triple.getPredicate().isVariable() && !triple.getObject().isVariable()) {
            object_filter = convertObject(triple.getObject());

            subject_map = "\""+triple.getSubject().toString()+"\"";
            predicate_map = "\""+triple.getPredicate().toString()+"\"";

            ArrayList<String> variables = new ArrayList<>();
            variables.add(subject_map);
            variables.add(predicate_map);
            SolutionMapping.insertSolutionMapping(indice, variables);

        } else if(triple.getSubject().isVariable() && !triple.getPredicate().isVariable() && !triple.getObject().isVariable()) {
            predicate_filter = "\""+triple.getPredicate().toString()+"\"";
            object_filter = convertObject(triple.getObject());

            subject_map = "\""+triple.getSubject().toString()+"\"";

            ArrayList<String> variables = new ArrayList<>();
            variables.add(subject_map);
            SolutionMapping.insertSolutionMapping(indice, variables);

        } else if(triple.getSubject().isVariable() && !triple.getPredicate().isVariable() && triple.getObject().isVariable()) {
            predicate_filter = "\""+triple.getPredicate().toString()+"\"";

            subject_map = "\""+triple.getSubject().toString()+"\"";
            object_map = "\""+triple.getObject().toString()+"\"";

            ArrayList<String> variables = new ArrayList<>();
            variables.add(subject_map);
            variables.add(object_map);
            SolutionMapping.insertSolutionMapping(indice, variables);

        } else if(!triple.getSubject().isVariable() && triple.getPredicate().isVariable() && triple.getObject().isVariable()) {
            subject_filter = "\""+triple.getSubject().toString()+"\"";

            predicate_map = "\""+triple.getPredicate().toString()+"\"";
            object_map = "\""+triple.getObject().toString()+"\"";

            ArrayList<String> variables = new ArrayList<>();
            variables.add(predicate_map);
            variables.add(object_map);
            SolutionMapping.insertSolutionMapping(indice, variables);

        } else if(!triple.getSubject().isVariable() && !triple.getPredicate().isVariable() && triple.getObject().isVariable()) {
            subject_filter = "\""+triple.getSubject().toString()+"\"";
            predicate_filter = "\""+triple.getPredicate().toString()+"\"";

            object_map = "\""+triple.getObject().toString()+"\"";

            ArrayList<String> variables = new ArrayList<>();
            variables.add(object_map);
            SolutionMapping.insertSolutionMapping(indice, variables);

        } else if(!triple.getSubject().isVariable() && !triple.getPredicate().isVariable() && !triple.getObject().isVariable()) {
            subject_filter = "\""+triple.getSubject().toString()+"\"";
            predicate_filter = "\""+triple.getPredicate().toString()+"\"";
            object_filter = convertObject(triple.getObject());

            SolutionMapping.insertSolutionMapping(indice, null);
        }

        filter_map += "\t\t\t.filter(new Triple2Triple("+subject_filter+", "+predicate_filter+", "+object_filter+"))\n";
        filter_map += "\t\t\t.map(new Triple2SolutionMapping("+subject_map+", "+predicate_map+", "+object_map+"));\n\n";

        return filter_map;
    }
}
