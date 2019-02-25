package sparql2flink.runner.functions;

import org.apache.flink.api.common.functions.FilterFunction;

//SolutionMapping to SolutionMapping - Filter Function
public class SM2SM_FF implements FilterFunction<SolutionMapping> {

    private String expression = null;

    public SM2SM_FF(String expression){
        this.expression = expression;
    }

    @Override
    public boolean filter(SolutionMapping sm) {
        return sm.filter(expression);
    }
}
