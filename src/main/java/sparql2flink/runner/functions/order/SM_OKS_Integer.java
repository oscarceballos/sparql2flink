package sparql2flink.runner.functions.order;

import org.apache.flink.api.java.functions.KeySelector;
import sparql2flink.runner.functions.SolutionMapping;

// SolutionMapping - Key Selector Order by
public class SM_OKS_Integer implements KeySelector<SolutionMapping, Integer> {

	private String key;

	public SM_OKS_Integer(String k) {
		this.key = k;
	}

	@Override
	public Integer getKey(SolutionMapping sm) {
		return Integer.parseInt(sm.getMapping().get(key).getLiteralValue().toString());
	}

}
