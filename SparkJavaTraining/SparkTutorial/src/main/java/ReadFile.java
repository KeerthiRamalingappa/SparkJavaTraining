import org.apache.spark.SparkConf;


import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public  class ReadFile {
	
	
	
//	public void hello(){
//		System.out.println("hello world");
//	}
	
	public void readCsvFile(){
	
	//SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
	
	
	//JavaSparkContext sc = new JavaSparkContext(conf);
		
		System.out.println("hello");
	
	SparkSession spark=SparkSession.builder().master("local").appName("assignment1").getOrCreate();
	//JavaSparkContext sc=new JavaSparkContext(spark.sparkContext());
	
	
	//SQLContext sqlContext= new SQLContext(sc);
	//SQLContext context = new SQLContext(spark);
	
	
	
	
	Dataset<Row> inputDf = spark.read().format("csv").option("inferSchema", "true")
			.option("header", "true")
			.load("C:\\Users\\Keerthi\\Documents\\SparkAssignments\\assignment1_accountdata.csv");
	
	
	inputDf.write().format("csv")
	.option("header", "true").save("C:\\Users\\Keerthi\\Documents\\SparkAssignments\\read_write.csv");
	}

}



