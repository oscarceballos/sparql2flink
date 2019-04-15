package sparql2flink.mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class LoadQueryFile {

    private String queryFile;

    public LoadQueryFile(String queryFile){
        this.queryFile = queryFile;
    }

    public String loadSQFile() {
        String line="", query="";
        try{
            File inputFile = new File(queryFile);
            FileReader in = new FileReader(inputFile);
            BufferedReader inputStream = new BufferedReader(in);
            while((line = inputStream.readLine()) != null) {
                query += line + "\n" ;
            }
            in.close();
        }catch(IOException e) {
            System.err.format("IOException: %s%n", e);
        }
        return query;
    }
}
