package sparql2flink.runner.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.jena.graph.Triple;

//Triple to SolutionMapping - Map Function
public class Triple2SolutionMapping implements MapFunction<Triple, SolutionMapping> {

    private String var_s, var_p, var_o = null;

    public Triple2SolutionMapping(String s, String p, String o){
        this.var_s = s;
        this.var_p = p;
        this.var_o = o;
    }

    @Override
    public SolutionMapping map(Triple t){
        SolutionMapping sm = new SolutionMapping();
        if(var_s!=null && var_p==null && var_o==null) {
            sm.putMapping(var_s, t.getSubject());
        } else if(var_s!=null && var_p!=null && var_o==null) {
            sm.putMapping(var_s, t.getSubject());
            sm.putMapping(var_p, t.getPredicate());
        } else if(var_s!=null && var_p==null && var_o!=null) {
            sm.putMapping(var_s, t.getSubject());
            sm.putMapping(var_o, t.getObject());
        } else if(var_s==null && var_p!=null && var_o==null) {
            sm.putMapping(var_p, t.getPredicate());
        } else if(var_s==null && var_p!=null && var_o!=null) {
            sm.putMapping(var_p, t.getPredicate());
            sm.putMapping(var_o, t.getObject());
        } else if(var_s==null && var_p==null && var_o!=null) {
            sm.putMapping(var_o, t.getObject());
        } else {
            sm.putMapping(var_s, t.getSubject());
            sm.putMapping(var_p, t.getPredicate());
            sm.putMapping(var_o, t.getObject());
        }
        return sm;
    }
}
