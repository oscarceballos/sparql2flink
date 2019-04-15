package sparql2flink.runner.functions;

import org.apache.flink.api.common.functions.FilterFunction;

//SolutionMapping to SolutionMapping - Filter Function
public class Filter implements FilterFunction<SolutionMapping> {

    private String expression = null;

    public Filter(String expression){
        this.expression = expression;
    }

    @Override
    public boolean filter(SolutionMapping sm) {
        return sm.filter(expression);
    }
}
