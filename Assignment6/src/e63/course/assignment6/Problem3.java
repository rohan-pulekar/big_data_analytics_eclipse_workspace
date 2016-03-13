package e63.course.assignment6;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Problem3 {
	public static void main(String args[]) {

		// set input files
		String ebayFilePath = args[0];

		// Create a Java Spark Config.
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Assignment4_Problem4_Basic");

		// Create a Java Spark Context
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		
		SQLContext sqlContext = new SQLContext(sparkContext)
	}
}
