package sparql2flink.runner.functions.filter;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.expr.*;

import java.util.HashMap;

//SolutionMapping - Filter Function
public class FilterConvert {

    public static boolean convert(E_Equals expression, HashMap<String, Node> hm) {
		Equals equals = new Equals(expression);
		return equals.eval(hm);
    }

    public static boolean convert(E_NotEquals expression, HashMap<String, Node> hm) {
		NotEquals notEquals = new NotEquals(expression);
		return notEquals.eval(hm);
    }

    public static boolean convert(E_GreaterThanOrEqual expression, HashMap<String, Node> hm) {
		GreaterThanOrEqual greaterThanOrEqual = new GreaterThanOrEqual(expression);
		return greaterThanOrEqual.eval(hm);
    }

    public static boolean convert(E_LessThanOrEqual expression, HashMap<String, Node> hm) {
		LessThanOrEqual lessThanOrEqual = new LessThanOrEqual(expression);
		return lessThanOrEqual.eval(hm);
    }

    public static boolean convert(E_GreaterThan expression, HashMap<String, Node> hm) {
		GreaterThan greaterThan = new GreaterThan(expression);
		return greaterThan.eval(hm);
    }

	public static boolean convert(E_LessThan expression, HashMap<String, Node> hm) {
		LessThan lessThan = new LessThan(expression);
		return lessThan.eval(hm);
	}

    public static boolean convert(E_LogicalAnd expression, HashMap<String, Node> hm) {
        return (convert(expression.getArg1(), hm) && convert(expression.getArg2(), hm));
    }

    public static boolean convert(E_LogicalOr expression, HashMap<String, Node> hm) {
		return (convert(expression.getArg1(), hm) || convert(expression.getArg2(), hm));
    }

    public static boolean convert(Expr expression, HashMap<String, Node> hm) {
        if (expression instanceof E_Equals) return convert((E_Equals) expression, hm);
        if (expression instanceof E_NotEquals) return convert((E_NotEquals) expression, hm);
        if (expression instanceof E_LessThan) return convert((E_LessThan) expression, hm);
        if (expression instanceof E_LessThanOrEqual) return convert((E_LessThanOrEqual) expression, hm);
        if (expression instanceof E_GreaterThan) return convert((E_GreaterThan) expression, hm);
        if (expression instanceof E_GreaterThanOrEqual) return convert((E_GreaterThanOrEqual) expression, hm);
        if (expression instanceof E_LogicalAnd) return convert((E_LogicalAnd) expression, hm);
        if (expression instanceof E_LogicalOr) return convert((E_LogicalOr) expression, hm);
        throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
    }
}
