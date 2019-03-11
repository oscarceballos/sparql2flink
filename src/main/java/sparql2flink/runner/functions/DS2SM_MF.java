package sparql2flink.runner.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.jena.graph.Node;

import java.util.Map;

//SolutionMapping to SolutionMapping - Map Function
public class DS2SM_MF implements MapFunction<SolutionMapping, Node> {

    private String label = null;

    public DS2SM_MF(String label){
        this.label= label;
    }

    @Override
    public Node map(SolutionMapping sm){
        System.out.println("Node: "+ sm.getMapping().get(label));
        return sm.getMapping().get(label);
    }
}

