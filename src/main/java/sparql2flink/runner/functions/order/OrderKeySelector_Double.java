package sparql2flink.runner.functions.order;

import org.apache.flink.api.java.functions.KeySelector;
import sparql2flink.runner.functions.SolutionMapping;

// SolutionMapping - Key Selector Order by
public class OrderKeySelector_Double implements KeySelector<SolutionMapping, Double> {

	private String key;

	public OrderKeySelector_Double(String k) {
		this.key = k;
	}

	@Override
	public Double getKey(SolutionMapping sm) {
		return Double.parseDouble(sm.getMapping().get(key).getLiteralValue().toString());
	}

}
