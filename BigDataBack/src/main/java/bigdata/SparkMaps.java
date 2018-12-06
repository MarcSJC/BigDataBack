package bigdata;

import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkMaps {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> rdd;
		rdd = context.textFile(args[0]);
		System.out.println(">>>>>>>>>>>>>>>>>>>> Old Nb Repartitions : " + rdd.getNumPartitions());
		rdd = rdd.repartition(4);
		System.out.println(">>>>>>>>>>>>>>>>>>>> New Nb Repartitions : " + rdd.getNumPartitions());
		rdd = rdd.filter((line) -> {
			String tab[] = line.split(",");
			return (!tab[4].isEmpty() && tab[4] != "" && tab[4].matches("\\d+"));
		});
		JavaRDD<Tuple2<String, Integer>> rdd2 = rdd.map((line) -> {
			String tab[] = line.split(",");
			return new Tuple2<String, Integer>(tab[2], Integer.valueOf(tab[4]));
		});
		rdd2.repartition(4);
		System.out.println(">>>>>>>>>>>>>>>>>>> count : " + rdd2.count());
	}	

	
}
