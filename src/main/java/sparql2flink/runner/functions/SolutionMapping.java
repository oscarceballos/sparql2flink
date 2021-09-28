package sparql2flink.runner.functions;

import sparql2flink.runner.functions.filter.FilterConvert;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.sse.SSE;

import java.util.HashMap;
import java.util.Map;

public class SolutionMapping {

	private HashMap<String, Node> mapping = new HashMap<>();

	public SolutionMapping() {}

	public void SolutionMapping(HashMap<String, Node> sm){
		this.mapping = sm;
	}

    public void setMapping(HashMap<String, Node> mapping){
        this.mapping = mapping;
    }

	public HashMap<String, Node> getMapping(){
		return mapping;
	}

	public void putMapping(String var, Node val) {
		mapping.put(var, val);
	}

	public Node getValue(String var){
		return mapping.get(var);
	}

	public boolean existMapping(String var, Node val){
		Boolean flag = false;
        if(mapping.containsKey(var)) {
            if(mapping.get(var).equals(val)) {
                flag = true;
            }
        }
		return flag;
	}

	public SolutionMapping join(SolutionMapping sm){
        for (Map.Entry<String, Node> hm : sm.getMapping().entrySet()) {
            if (!existMapping(hm.getKey(), hm.getValue())) {
                this.putMapping(hm.getKey(), hm.getValue());
            }
        }
		return this;
	}

    public SolutionMapping leftJoin(SolutionMapping sm) {
        if(sm != null) {
            for (Map.Entry<String, Node> hm : sm.getMapping().entrySet()) {
                if (!existMapping(hm.getKey(), hm.getValue())) {
                    this.putMapping(hm.getKey(), hm.getValue());
                }
            }
        }
        return this;
    }

    public SolutionMapping project(String[] vars){
        SolutionMapping sm = new SolutionMapping();
        for (String var : vars) {
            if(mapping.containsKey(var)) {
                sm.putMapping(var, mapping.get(var));
            }
        }
        return sm;
    }

	public boolean filter(String expression){
		Expr expr = SSE.parseExpr(expression);
		return FilterConvert.convert(expr, this.getMapping());
	}

	public SolutionMapping distinct(String[] vars){
        SolutionMapping sm = new SolutionMapping();
        for (String var : vars) {
            if(mapping.containsKey(var)) {
                sm.putMapping(var, mapping.get(var));
            }
        }
        return sm;
	}

    public SolutionMapping newSolutionMapping(String[] vars){
        SolutionMapping sm = new SolutionMapping();
        for (String var : vars) {
            if(var != null) {
                sm.putMapping(var, mapping.get(var));
            }
        }
        return sm;
    }

	@Override
	public String toString() {
		String sm="";
		for (Map.Entry<String, Node> hm : mapping.entrySet()) {
		    if(hm.getValue() != null) {
                sm += hm.getKey() + "-->" + hm.getValue().toString() + "\t";
            }
		}
		return sm;
	}
}
