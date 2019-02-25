package sparql2flink.mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class SolutionMapping {

    private static HashMap<Integer, ArrayList<String>> solutionMapping = new HashMap<>();
    public static int indice = 1;

    public static ArrayList<String> getKey(int indice_sm_left, int indice_sm_right){
        ArrayList<String> listKeys = new ArrayList<>();
        for (String key_left : solutionMapping.get(indice_sm_left)) {
            for (String key_right : solutionMapping.get(indice_sm_right)) {
                if (key_left.equals(key_right)) {
                    listKeys.add(key_left);
                }
            }
        }
        return listKeys;
    }

    public static void join(int indice_sm, int indice_sm_left, int indice_sm_right){
        ArrayList<String> variables = new ArrayList<>();

        for (String varSML : solutionMapping.get(indice_sm_left)) {
            variables.add(varSML);
        }

        for (String varSMR : solutionMapping.get(indice_sm_right)) {
            if (!(variables.contains(varSMR))) {
                variables.add(varSMR);
            }
        }
        insertSolutionMapping(indice_sm, variables);

    }

    public static void insertSolutionMapping(Integer indice_sm, ArrayList<String> variables){
        solutionMapping.put(indice_sm, variables);
        indice++;
    }

    public static int getIndice(){
        return indice;
    }

    public static HashMap<Integer, ArrayList<String>> getSolutionMapping(){
        return solutionMapping;
    }

    @Override
    public String toString() {
        String sm="";
        for (Map.Entry<Integer, ArrayList<String>> hm : solutionMapping.entrySet()) {
            sm += hm.getKey() + "-->" + hm.getValue().toString() + "\t";
        }
        return sm;
    }
}
