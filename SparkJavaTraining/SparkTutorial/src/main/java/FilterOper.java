import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FilterOper implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	
	public void removeRecord(){
		
    //String outputDirectory="C:\\Users\\Keerthi\\Documents\\SparkAssignments\\assignment_1accountdata1.csv";
		
	SparkSession spark = SparkSession.builder().master("local").appName("assignment1")
			.config("spark.sql.warehouse.dir", "C:\\Users\\Keerthi\\spark").getOrCreate();

	JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

	JavaRDD<String> inputRDD = sc
			.textFile("C:\\Users\\Keerthi\\Documents\\SparkAssignments\\assignment_1accountdata1.csv");

	//if a record of Code="c" is matched, then true value is returned and that particular value is returned
	
	
	
	JavaRDD<String> outputRDD = inputRDD.filter(new Function<String, Boolean>() {

		public Boolean call(String code)

		{
			return (code.contains("c"));
		}
	});
	
	String path="C:\\Users\\Keerthi\\Documents\\SparkAssignments\\output1.csv";
	outputRDD.saveAsTextFile(path);
	}
	
	
}

