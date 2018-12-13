package bigdata;

import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;

import scala.Tuple2;

public class SparkMaps {
	
	final static int dem3Size= 1201;

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SparkMaps");
		JavaSparkContext context = new JavaSparkContext(conf);
		int data[][] = new int[dem3Size][dem3Size];
		JavaPairRDD<String, PortableDataStream> rdd;
		rdd = context.binaryFiles(args[0]);
		rdd = rdd.repartition(4);
		System.out.println(">>>>>>>>>>>>>>>>>>> count : " + rdd.count());
		System.out.println(">>>>>>>>>>>>>>>>>>> example id : " + rdd.id());
		System.out.println(">>>>>>>>>>>>>>>>>>> example name : " + rdd.name());
	}	

	
}
