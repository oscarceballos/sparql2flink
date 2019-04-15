package sparql2flink.runner.functions;

import org.apache.flink.api.common.functions.MapFunction;

//SolutionMapping to SolutionMapping - Map Function
public class SolutionMapping2SolutionMapping implements MapFunction<SolutionMapping, SolutionMapping> {

    private String[] vars = null;

    public SolutionMapping2SolutionMapping(String[] vars){
        this.vars = vars;
    }

    @Override
    public SolutionMapping map(SolutionMapping sm){
        return sm.newSolutionMapping(vars);
    }
}

