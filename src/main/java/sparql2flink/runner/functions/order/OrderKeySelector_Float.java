package sparql2flink.runner.functions.order;

import org.apache.flink.api.java.functions.KeySelector;
import sparql2flink.runner.functions.SolutionMapping;


// SolutionMapping - Key Selector Order by
public class OrderKeySelector_Float implements KeySelector<SolutionMapping, Float> {

	private String key;

	public OrderKeySelector_Float(String k) {
		this.key = k;
	}

	@Override
	public Float getKey(SolutionMapping sm) {
		return Float.parseFloat(sm.getMapping().get(key).getLiteralValue().toString());
	}

}
