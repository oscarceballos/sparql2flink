package sparql2flink.mapper;

import java.util.ArrayList;
import java.util.Iterator;

public class JoinKeys {

    public static String keys(ArrayList<String> listKeys){
        String keys="";
        Iterator<String> iter = listKeys.iterator();
        for (; iter.hasNext(); ) {
            String key = iter.next();
            keys += key;
            if(iter.hasNext()){
                keys += ", ";
            }
        }
        return keys;
    }

}
