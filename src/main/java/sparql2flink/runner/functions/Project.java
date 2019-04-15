package sparql2flink.runner.functions;

import org.apache.flink.api.common.functions.MapFunction;

//SolutionMapping to SolutionMapping - Map Function
public class Project implements MapFunction<SolutionMapping, SolutionMapping> {

    private String[] vars = null;

    public Project(String[] vars){
        this.vars = vars;
    }

    @Override
    public SolutionMapping map(SolutionMapping sm){
        return sm.project(vars);
    }
}

