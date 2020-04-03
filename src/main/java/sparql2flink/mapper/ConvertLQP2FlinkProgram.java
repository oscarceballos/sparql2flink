package sparql2flink.mapper;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.SortCondition;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ConvertLQP2FlinkProgram extends OpVisitorBase {

    private static String flinkProgram = "";

    public  ConvertLQP2FlinkProgram() {
        super();
    }

    @Override
    public void visit(OpBGP opBGP) {
        List<Triple> listTriplePatterns = opBGP.getPattern().getList();
        flinkProgram += ConvertTriplePatternGroup.convert(listTriplePatterns);
    }

    @Override
    public void visit(OpJoin opJoin) {
        Op opLeft = opJoin.getLeft();
        Op opRight = opJoin.getRight();

        opLeft.visit(this);
        int indice_sm_left = SolutionMapping.getIndice()-1;

        opRight.visit(this);
        int indice_sm_right = SolutionMapping.getIndice()-1;

        int indice_sm_join = SolutionMapping.getIndice();

        ArrayList<String> listKeys = SolutionMapping.getKey(indice_sm_left, indice_sm_right);

        if(listKeys.size()>0) {
            String keys = JoinKeys.keys(listKeys);
            flinkProgram += "\t\tDataSet<SolutionMapping> sm" + indice_sm_join + " = sm" + indice_sm_left + ".join(sm" + indice_sm_right + ")\n" +
                    "\t\t\t.where(new JoinKeySelector(new String[]{"+keys+"}))\n" +
                    "\t\t\t.equalTo(new JoinKeySelector(new String[]{"+keys+"}))\n" +
                    "\t\t\t.with(new Join());" +
                    "\n\n";
        }
        else {
            flinkProgram += "\t\tDataSet<SolutionMapping> sm" + indice_sm_join + " = sm" + indice_sm_left + ".cross(sm" + indice_sm_right + ")\n" +
                    "\t\t\t.with(new Cross());" +
                    "\n\n";
            }

        SolutionMapping.join(indice_sm_join, indice_sm_left, indice_sm_right);
    }

    @Override
    public void visit(OpLeftJoin opLeftJoin) {
        Op opLeft = opLeftJoin.getLeft();
        Op opRight = opLeftJoin.getRight();

        opLeft.visit(this);
        int indice_sm_left = SolutionMapping.getIndice()-1;

        opRight.visit(this);
        int indice_sm_right = SolutionMapping.getIndice()-1;

        int indice_sm_join = SolutionMapping.getIndice();

        ArrayList<String> listKeys = SolutionMapping.getKey(indice_sm_left, indice_sm_right);

        if(listKeys.size()>0) {
            String keys = JoinKeys.keys(listKeys);
            flinkProgram += "\t\tDataSet<SolutionMapping> sm" + indice_sm_join + " = sm" + indice_sm_left + ".leftOuterJoin(sm" + indice_sm_right + ")\n" +
                    "\t\t\t.where(new JoinKeySelector(new String[]{"+keys+"}))\n" +
                    "\t\t\t.equalTo(new JoinKeySelector(new String[]{"+keys+"}))\n" +
                    "\t\t\t.with(new LeftJoin());" +
                    "\n\n";
        }
        else {
            flinkProgram += "\t\tDataSet<SolutionMapping> sm"+indice_sm_join+" = sm"+indice_sm_left+".cross(sm"+indice_sm_right+")\n" +
                    "\t\t\t.with(new Cross());" +
                    "\n\n";
        }

        SolutionMapping.join(indice_sm_join, indice_sm_left, indice_sm_right);

        if(opLeftJoin.getExprs() != null) {
            this.visit(opLeftJoin.getExprs());
        }
    }

    @Override
    public void visit(OpUnion opUnion) {
        Op opLeft = opUnion.getLeft();
        Op opRight = opUnion.getRight();

        opLeft.visit(this);
        int indice_sm_left = SolutionMapping.getIndice()-1;

        opRight.visit(this);
        int indice_sm_right = SolutionMapping.getIndice()-1;

        int indice_sm_join = SolutionMapping.getIndice();

        flinkProgram += "\t\tDataSet<SolutionMapping> sm"+indice_sm_join+" = sm"+indice_sm_left+".union(sm"+indice_sm_right+");" +
                "\n\n";

        SolutionMapping.join(indice_sm_join, indice_sm_left, indice_sm_right);
    }

    @Override
    public void visit(OpProject opProject) {
        ArrayList<String> variables = new ArrayList<>();

        String varsProject = "";
        Iterator<Var> iter = opProject.getVars().iterator();
        for (; iter.hasNext(); ) {
            String var = "\"?"+iter.next().getVarName()+"\"";
            varsProject += var;
            if(iter.hasNext()){
                varsProject += ", ";
            }
            variables.add(var);
        }

        opProject.getSubOp().visit(this);

        flinkProgram += "\t\tDataSet<SolutionMapping> sm"+(SolutionMapping.getIndice())+" = sm"+(SolutionMapping.getIndice()-1)+"\n" +
                "\t\t\t.map(new Project(new String[]{"+varsProject+"}));\n\n";

        SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
    }

    @Override
    public void visit(OpFilter opFilter) {
        ExprList exprList = opFilter.getExprs();
        opFilter.getSubOp().visit(this);
        for ( Expr expression : exprList ) {
            flinkProgram += "\t\tDataSet<SolutionMapping> sm"+(SolutionMapping.getIndice())+" = sm"+(SolutionMapping.getIndice()-1)+"\n" +
                    "\t\t\t.filter(new Filter(\""+FilterConvert.convert(expression)+"\"));\n\n";

            ArrayList<String> variables = SolutionMapping.getSolutionMapping().get(SolutionMapping.getIndice()-1);

            SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
        }
    }

    public void visit(ExprList exprList) {
        for ( Expr expression : exprList ) {
            flinkProgram += "\t\tDataSet<SolutionMapping> sm"+(SolutionMapping.getIndice())+" = sm"+(SolutionMapping.getIndice()-1)+"\n" +
                    "\t\t\t.filter(new Filter(\""+FilterConvert.convert(expression)+"\"));\n\n";

            ArrayList<String> variables = SolutionMapping.getSolutionMapping().get(SolutionMapping.getIndice()-1);

            SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
        }
    }

    @Override
    public void visit(OpDistinct opDistinct) {
        opDistinct.getSubOp().visit(this);

        flinkProgram += "\t\tDataSet<SolutionMapping> sm"+(SolutionMapping.getIndice())+" = sm"+(SolutionMapping.getIndice()-1)+"\n" +
                "\t\t\t.distinct(new DistinctKeySelector());\n\n";

        ArrayList<String> variables = SolutionMapping.getSolutionMapping().get(SolutionMapping.getIndice()-1);

        SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
    }

    @Override
    public void visit(OpOrder opOrder) {
        List<SortCondition> sortCondition = opOrder.getConditions();

        String order="";
        if(sortCondition.get(0).getDirection()==-2) {
            order = "Order.ASCENDING";
        } else if (sortCondition.get(0).getDirection()==-1) {
            order = "Order.DESCENDING";
        }

        opOrder.getSubOp().visit(this);

        Expr expression = sortCondition.get(0).getExpression();

        flinkProgram += "\t\tDataSet<SolutionMapping> sm"+SolutionMapping.getIndice()+" = sm"+(SolutionMapping.getIndice()-1)+"\n" +
                "\t\t\t\t\t.sortPartition(new OrderKeySelector(\""+expression+"\"), "+order+")\n" +
                "\t\t\t\t\t.setParallelism(1);\n" +
                "\t\n";

        /*flinkProgram += "\t\tDataSet<SolutionMapping> sm"+SolutionMapping.getIndice()+";\n" +
                "\t\tNode node = sm"+(SolutionMapping.getIndice()-1)+".collect().get(0).getValue(\""+expression+"\");\n" +
                "\t\tif(node.isLiteral()) {\n" +
                "\t\t\tif(node.getLiteralValue().getClass().equals(BigDecimal.class) || node.getLiteralValue().getClass().equals(Double.class)){\n" +
                "\t\t\t\tsm"+SolutionMapping.getIndice()+" = sm"+(SolutionMapping.getIndice()-1)+"\n" +
                "\t\t\t\t\t.sortPartition(new OrderKeySelector_Double(\""+expression+"\"), "+order+")\n" +
                "\t\t\t\t\t.setParallelism(1);\n" +
                "\t\t\t} else if (node.getLiteralValue().getClass().equals(BigInteger.class) || node.getLiteralValue().getClass().equals(Integer.class)) {\n" +
                "\t\t\t\tsm"+SolutionMapping.getIndice()+" = sm"+(SolutionMapping.getIndice()-1)+"\n" +
                "\t\t\t\t\t.sortPartition(new OrderKeySelector_Integer(\""+expression+"\"), "+order+")\n" +
                "\t\t\t\t\t.setParallelism(1);\n" +
                "\t\t\t} else if (node.getLiteralValue().getClass().equals(Float.class)) {\n" +
                "\t\t\t\tsm"+SolutionMapping.getIndice()+" = sm"+(SolutionMapping.getIndice()-1)+"\n" +
                "\t\t\t\t\t.sortPartition(new OrderKeySelector_Float(\""+expression+"\"), "+order+")\n" +
                "\t\t\t\t\t.setParallelism(1);\n" +
                "\t\t\t} else if (node.getLiteralValue().getClass().equals(Long.class)){\n" +
                "\t\t\t\tsm"+SolutionMapping.getIndice()+" = sm"+(SolutionMapping.getIndice()-1)+"\n" +
                "\t\t\t\t\t.sortPartition(new OrderKeySelector_Long(\""+expression+"\"), "+order+")\n" +
                "\t\t\t\t\t.setParallelism(1);\n" +
                "\t\t\t} else {\n" +
                "\t\t\t\tsm"+SolutionMapping.getIndice()+" = sm"+(SolutionMapping.getIndice()-1)+"\n" +
                "\t\t\t\t\t.sortPartition(new OrderKeySelector_String(\""+expression+"\"), "+order+")\n" +
                "\t\t\t\t\t.setParallelism(1);\n" +
                "\t\t\t}\n" +
                "\t\t} else {\n" +
                "\t\t\t\tsm"+SolutionMapping.getIndice()+" = sm"+(SolutionMapping.getIndice()-1)+"\n" +
                "\t\t\t\t\t.sortPartition(new OrderKeySelector_String(\""+expression+"\"), "+order+")\n" +
                "\t\t\t\t\t.setParallelism(1);\n" +
                "\t\t}\n\n";*/

        ArrayList<String> variables = SolutionMapping.getSolutionMapping().get(SolutionMapping.getIndice()-1);

        SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
    }


    @Override
    public void visit(OpSlice opSlice) {
        opSlice.getSubOp().visit(this);

        flinkProgram += "\t\tDataSet<SolutionMapping> sm"+(SolutionMapping.getIndice())+" = sm"+(SolutionMapping.getIndice()-1)+"\n" +
                "\t\t\t.first("+opSlice.getLength()+");\n\n";

        ArrayList<String> variables = SolutionMapping.getSolutionMapping().get(SolutionMapping.getIndice()-1);

        SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
    }

    public static String getFlinkProgram(){
        return flinkProgram;
    }
}
