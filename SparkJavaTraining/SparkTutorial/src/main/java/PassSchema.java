import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.DataType;
import org.apache.spark.sql.api.java.StructField;
import org.apache.spark.sql.api.java.StructType;
//import org.apache.spark.sql.types.DataTypes;
//import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.DataFrame;
public class PassSchema extends StructType{
	
	PassSchema(StructField[] structField) {
		super(structField); 
	}
	
	
	
		SparkSession spark = SparkSession.builder().master("local").appName("assignment1")
				.config("spark.sql.warehouse.dir", "C:\\Users\\Keerthi\\spark").getOrCreate();
		
		Dataset<Row> dataf = spark.read().format("csv").option("inferSchema", "true")
				.option("header", "true")
				.load("C:\\Users\\Keerthi\\Documents\\SparkAssignments\\assignment_1accountdata.csv");
		
		JavaRDD<Row> row= dataf.toJavaRDD();
		
//		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
//
//		JavaRDD<String> inputRDD = sc
//				.textFile("C:\\Users\\Keerthi\\Documents\\SparkAssignments\\assignment_1accountdata1.csv", 1);
		
//		System.out.println("print schema");
		//df.printSchema();
		
//		df.toJavaRDD();
//		df.printSchema();
//		System.out.println("length " +df.head().length());
		
		
		
		
	
	
	//Build and return a schema to use for the csv data in the form of Row Object
	
	public StructType buildSchema(){
		
		
		StructField[] structField = new StructField[]{
				DataType.createStructField("id", DataType.IntegerType, true),
				DataType.createStructField("code", DataType.StringType, true),
				DataType.createStructField("subvalues", DataType.StringType, true),
				DataType.createStructField("comment", DataType.StringType, true),
				DataType.createStructField("datadt", DataType.IntegerType, true),
		};
		PassSchema schema = new PassSchema(structField); 
		return (schema);
		
	}
	
	
	
	
	
	
	DataFrame df=sqlContext.createDataFrame(row,buildSchema());

	
	
	
	
	

	
}