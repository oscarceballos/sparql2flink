package sparql2flink.runner.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

//Triple to SolutionMapping - Map Function
public class Triple2SolutionMapping2 implements FlatMapFunction<Triple, SolutionMapping> {

    private String subject, predicate, object = null;

    public Triple2SolutionMapping2(String s, String p, String o){
        this.subject = s;
        this.predicate = p;
        this.object = o;
    }

    public boolean evalObject(Node node){
        Boolean flag = false;
        if(node.isLiteral()) {
            if (node.getLiteralValue().toString().equals(object)){
                flag = true;
            }
        } else if (node.isURI()) {
            if (node.getURI().toString().equals(object)) {
                flag = true;
            }
        }
        return flag;
    }

    @Override
    public void flatMap(Triple t, Collector<SolutionMapping> out){
        if(subject.contains("?") && !predicate.contains("?") && !object.contains("?")) {
            if(t.getPredicate().toString().equals(predicate) && evalObject(t.getObject())) {
                SolutionMapping sm = new SolutionMapping();
                sm.putMapping(subject, t.getSubject());
                out.collect(sm);
            }
        } else if(!subject.contains("?") && predicate.contains("?") && !object.contains("?")) {
            if(t.getSubject().toString().equals(subject) && evalObject(t.getObject())) {
                SolutionMapping sm = new SolutionMapping();
                sm.putMapping(predicate, t.getPredicate());
                out.collect(sm);
            }
        } else if(!subject.contains("?") && !predicate.contains("?") && object.contains("?")) {
            if(t.getSubject().toString().equals(subject) && t.getPredicate().toString().equals(predicate)) {
                SolutionMapping sm = new SolutionMapping();
                sm.putMapping(object, t.getObject());
                out.collect(sm);
            }
        } else if(!subject.contains("?") && predicate.contains("?") && object.contains("?")) {
            if(t.getSubject().toString().equals(subject)) {
                SolutionMapping sm = new SolutionMapping();
                sm.putMapping(predicate, t.getPredicate());
                sm.putMapping(object, t.getObject());
                out.collect(sm);
            }
        } else if(subject.contains("?") && !predicate.contains("?") && object.contains("?")) {
            if(t.getPredicate().toString().equals(predicate)) {
                SolutionMapping sm = new SolutionMapping();
                sm.putMapping(subject, t.getSubject());
                sm.putMapping(object, t.getObject());
                out.collect(sm);
            }
        } else if(subject.contains("?") && predicate.contains("?") && !object.contains("?")) {
            if(evalObject(t.getObject())) {
                SolutionMapping sm = new SolutionMapping();
                sm.putMapping(subject, t.getSubject());
                sm.putMapping(predicate, t.getPredicate());
                out.collect(sm);
            }
        } else {
            SolutionMapping sm = new SolutionMapping();
            sm.putMapping(subject, t.getSubject());
            sm.putMapping(predicate, t.getPredicate());
            sm.putMapping(object, t.getObject());
            out.collect(sm);
        }
    }
}
