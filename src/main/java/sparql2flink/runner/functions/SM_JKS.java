package sparql2flink.runner.functions;

import org.apache.flink.api.java.functions.KeySelector;

// SolutionMapping - Key Selector Join
public class SM_JKS implements KeySelector<SolutionMapping, String> {

    private String[] keys;

    public SM_JKS(String[] keys){
        this.keys = keys;
    }

    @Override
    public String getKey(SolutionMapping sm) {
        String value ="";
        int i=0;
        for (String key : keys) {
            value += sm.getMapping().get(key).toString();
            if(++i < keys.length) {
                value += ",";
            }
        }
		return value;
    }
}
