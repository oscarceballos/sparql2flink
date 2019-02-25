package sparql2flink.runner.functions.order;

import org.apache.flink.api.java.functions.KeySelector;
import sparql2flink.runner.functions.SolutionMapping;

// SolutionMapping - Key Selector Order by
public class SM_OKS_Long implements KeySelector<SolutionMapping, Long> {

	private String key;

	public SM_OKS_Long(String k) {
		this.key = k;
	}

	@Override
	public Long getKey(SolutionMapping sm) {
		return Long.parseLong(sm.getMapping().get(key).getLiteralValue().toString());
	}

}
