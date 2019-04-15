package sparql2flink.runner.functions;

import org.apache.flink.api.common.functions.CrossFunction;

//SolutionMapping - Cross Function
public class Cross implements CrossFunction<SolutionMapping, SolutionMapping, SolutionMapping> {

    @Override
    public SolutionMapping cross(SolutionMapping left, SolutionMapping right) {
        return left.join(right);
    }
}
