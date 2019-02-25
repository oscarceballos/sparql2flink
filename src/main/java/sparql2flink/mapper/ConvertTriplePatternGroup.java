package sparql2flink.mapper;

import org.apache.jena.graph.Triple;

import java.util.ArrayList;
import java.util.List;

public class ConvertTriplePatternGroup {

    public ConvertTriplePatternGroup() { }

    public static String joinSolutionMapping (int indice_sm_join, int indice_sm_left, int indice_sm_right) {
        String sm = "";
        ArrayList<String> listKeys = SolutionMapping.getKey(indice_sm_left, indice_sm_right);
        if(listKeys.size()>0) {
            String keys = JoinKeys.keys(listKeys);
            sm = "\t\tDataSet<SolutionMapping> sm" + indice_sm_join + " = sm" + indice_sm_left + ".join(sm" + indice_sm_right + ")\n" +
                    "\t\t\t.where(new SM_JKS(new String[]{"+keys+"}))\n" +
                    "\t\t\t.equalTo(new SM_JKS(new String[]{"+keys+"}))\n" +
                    "\t\t\t.with(new SM_JF());" +
                    "\n\n";
        } else {
            sm = "\t\tDataSet<SolutionMapping> sm" + indice_sm_join + " = sm" + indice_sm_left + ".cross(sm" + indice_sm_right + ")\n" +
                    "\t\t\t.with(new SM_CF());" +
                    "\n\n";
        }
        SolutionMapping.join(indice_sm_join, indice_sm_left, indice_sm_right);
        return sm;
    }

    public static String convertTPG(List<Triple> listTriplePatterns, int indiceLTP, int count, int indiceSM, String bgp) {
        if (indiceLTP>=listTriplePatterns.size() && count==1) {
            return bgp;
        } else {
            if(count == 2) {
                bgp += joinSolutionMapping(indiceSM,indiceSM-2, indiceSM-1);
                count = 1;
            } else {
                bgp += "\t\tDataSet<SolutionMapping> sm" + indiceSM + " = dataset\n" +
                    ConvertTriplePattern.convert(listTriplePatterns.get(indiceLTP), indiceSM);
                indiceLTP += 1;
                count += 1;
            }
            return convertTPG(listTriplePatterns, indiceLTP, count, SolutionMapping.getIndice(), bgp);
        }
    }

    public static String convert(List<Triple> listTriplePatterns){
        return convertTPG(listTriplePatterns, 0, 0, SolutionMapping.getIndice(), "");
    }
}
