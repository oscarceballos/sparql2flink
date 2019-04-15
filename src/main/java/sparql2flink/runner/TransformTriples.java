package sparql2flink.runner;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Preconditions;
import org.apache.jena.graph.Triple;


public class TransformTriples {

	public static DataSet<Triple> loadTriplesFromDataset(ExecutionEnvironment environment, String filePath) {
		Preconditions.checkNotNull(filePath, "The file path may not be null.");

        DataSource<String> dataSource = environment.readTextFile(filePath);

        DataSet<Triple> dataSet = dataSource.map(new String2Triple());

        return dataSet;
	}
}


