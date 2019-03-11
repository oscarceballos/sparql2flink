package sparql2flink.runner.functions.order;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.jena.graph.Node;
import sparql2flink.runner.functions.SolutionMapping;

import java.math.BigDecimal;

// SolutionMapping - Key Selector Order by
public class SM_OKS implements KeySelector<SolutionMapping, Object> {

	private String key;

	public SM_OKS(String k) {
		this.key = k;
	}

	@Override
	public Object getKey(SolutionMapping sm) {
        Object obj = null;
        Node node = sm.getMapping().get(key);
        if(node.isLiteral()) {
            if (node.getLiteralValue().getClass().equals(BigDecimal.class)) {
                obj = Double.parseDouble(node.getLiteralValue().toString());
            } else if (node.getLiteralValue().getClass().equals(Integer.class)) {
                obj = Integer.parseInt(node.getLiteralValue().toString());
            } else if (node.getLiteralValue().getClass().equals(Float.class)) {
                obj = Float.parseFloat(node.getLiteralValue().toString());
            } else if (node.getLiteralValue().getClass().equals(Long.class)) {
                obj = Long.parseLong(node.getLiteralValue().toString());
            } else {
                obj = node.getLiteralValue().toString();
            }
        }else {
            if(node.isURI()){
                obj = node.getURI().toString();
            }
        }
        return obj;
	}
}
