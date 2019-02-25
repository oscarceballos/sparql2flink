package sparql2flink.mapper;

import org.apache.jena.sparql.expr.*;

public class FilterConvert {

    public static String convertArgument(NodeValue node){
        String value="";
        if (node.isInteger() || node.isFloat() || node.isDouble()) {
            value = node.getNode().getLiteralValue().toString();
        } else if (node.isDateTime() || node.isDate() || node.isString()) {
            value = "\\\""+node.getNode().getLiteralValue().toString()+"\\\"";
        }
        return value;
    }


    public static String convert(E_Equals expression) {
        String exp = "";
        if(expression.getArg2().isConstant()) {
            exp = "(= "+expression.getArg1()+" "+convertArgument(expression.getArg2().getConstant())+")";
        } else if (expression.getArg2().isVariable()) {
            exp = expression.toString();
        }
        return exp;
    }

    public static String convert(E_NotEquals expression) {
        String exp = "";
        if(expression.getArg2().isConstant()) {
            exp = "(!= "+expression.getArg1()+" "+convertArgument(expression.getArg2().getConstant())+")";
        } else if (expression.getArg2().isVariable()) {
            exp = expression.toString();
        }
        return exp;
    }

    public static String convert(E_GreaterThanOrEqual expression) {
        String exp = "";
        if(expression.getArg2().isConstant()) {
            exp = "(>= "+expression.getArg1()+" "+convertArgument(expression.getArg2().getConstant())+")";
        } else if (expression.getArg2().isVariable()) {
            exp = expression.toString();
        }
        return exp;
    }

    public static String convert(E_LessThanOrEqual expression) {
        String exp = "";
        if(expression.getArg2().isConstant()) {
            exp = "(<= "+expression.getArg1()+" "+convertArgument(expression.getArg2().getConstant())+")";
        } else if (expression.getArg2().isVariable()) {
            exp = expression.toString();
        }
        return exp;
    }

    public static String convert(E_GreaterThan expression) {
        String exp = "";
        if(expression.getArg2().isConstant()) {
            exp = "(> "+expression.getArg1()+" "+convertArgument(expression.getArg2().getConstant())+")";
        } else if (expression.getArg2().isVariable()) {
            exp = expression.toString();
        }
        return exp;
    }

	public static String convert(E_LessThan expression) {
        String exp = "";
        if(expression.getArg2().isConstant()) {
            exp = "(< "+expression.getArg1()+" "+convertArgument(expression.getArg2().getConstant())+")";
        } else if (expression.getArg2().isVariable()) {
            exp = expression.toString();
        }
        return exp;
	}

    public static String convert(E_LogicalAnd expression) {
        return "(&& "+convert(expression.getArg1())+" "+convert(expression.getArg2())+")";
    }

    public static String convert(E_LogicalOr expression) {
        return "(|| "+convert(expression.getArg1())+" "+convert(expression.getArg2())+")";
    }

    public static String convert(Expr expression) {
        if (expression instanceof E_Equals) return convert((E_Equals) expression);
        if (expression instanceof E_NotEquals) return convert((E_NotEquals) expression);
        if (expression instanceof E_LessThan) return convert((E_LessThan) expression);
        if (expression instanceof E_LessThanOrEqual) return convert((E_LessThanOrEqual) expression);
        if (expression instanceof E_GreaterThan) return convert((E_GreaterThan) expression);
        if (expression instanceof E_GreaterThanOrEqual) return convert((E_GreaterThanOrEqual) expression);
        if (expression instanceof E_LogicalAnd) return convert((E_LogicalAnd) expression);
        if (expression instanceof E_LogicalOr) return convert((E_LogicalOr) expression);
        throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
    }
}
