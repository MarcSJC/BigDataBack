package bigdata;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;

import scala.Tuple2;

public class SparkMaps {
	
	final static int dem3Size= 1201;
	static int minh = 0;
	static int maxh = 255;

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SparkMaps");
		JavaSparkContext context = new JavaSparkContext(conf);
		int data[][] = new int[dem3Size][dem3Size];
		JavaRDD<byte[]> rdd;
		String filePath = args[0];
		rdd = context.binaryRecords(filePath, 2);
		rdd = rdd.repartition(4);
		byte buffer[] = new byte[2];
		double lat, lng;
		String s = filePath.substring(filePath.length() - 11, filePath.length());
		lat = Double.parseDouble(s.substring(1, 3));
		lng = Double.parseDouble(s.substring(4, 7));
		if (filePath.charAt(0) == 'S' || filePath.charAt(0) == 's') lat *= -1;
        if (filePath.charAt(3) == 'W' || filePath.charAt(3) == 'w') lng *= -1;
		System.out.println(">>>>>>>>>>>>>>>>>>> lat, lng : " + lat + ", " + lng);
		/*for (int i = 0 ; i < dem3Size ; ++i) {
			for (int j = 0 ; j < dem3Size ; ++j) {
				
			}
		}*/
		int id = 0;
		int i = 0;
		int j = 0;
		for (byte[] b : rdd.collect()) {
			int value= 0;
		    for(int k = 0 ; k < b.length ; k++)
		    	value = (value << 8) | b[k];
			value = Integer.min(value, maxh);
			value = Integer.max(value, minh);
			minh = Integer.min(value, minh);
			data[i][j] = value;
			if (j >= dem3Size - 1) {
				i++;
				j = 0;
			}
			else {
				j++;
			}
			if (i >= dem3Size - 1) {
				break;
			}
		}
		maxh -= minh;
		/*for (String line : rdd.collect()) {
			System.out.println(">>>>>>>>>>>>>>>>>>> truc : " + line);
		}*/
		System.out.println(">>>>>>>>>>>>>>>>>>>> data : " + Arrays.deepToString(data));
	}	

	
}
