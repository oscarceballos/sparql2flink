package sparql2flink.runner.functions;

import org.apache.flink.api.java.functions.KeySelector;

// SolutionMapping - Distinct Key Selector
public class SM_DKS implements KeySelector<SolutionMapping, String> {

    @Override
    public String getKey(SolutionMapping sm) {
        return sm.getMapping().toString();
    }
}
