import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FilterOper {
	
	
	SparkSession spark=SparkSession.builder().master("local").appName("assignment1").config("spark.sql.warehouse.dir","C:\\Users\\Keerthi\\spark").getOrCreate();
    
//	
//	 inputRDD = spark.read().format("csv").option("inferSchema", "true")
//			.option("header", "true")
//			.load("C:\\Users\\Keerthi\\Documents\\SparkAssignments\\assignment_1accountdata.csv");
	JavaSparkContext sc=new JavaSparkContext(spark.sparkContext());
	
	    
	
	JavaRDD<String> inputRDD = sc.textFile("C:\\Users\\Keerthi\\Documents\\SparkAssignments\\assignment_1accountdata.csv");
	
	JavaRDD<String> outputRDD = inputRDD.filter(f);
	
	
}
