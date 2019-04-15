package sparql2flink.runner.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;

//SolutionMapping - Flat Join Function
public class LeftJoin implements FlatJoinFunction<SolutionMapping, SolutionMapping, SolutionMapping> {

    @Override
    public void join(SolutionMapping left, SolutionMapping right, Collector<SolutionMapping> out) throws Exception {
        out.collect(left.leftJoin(right));
    }
}