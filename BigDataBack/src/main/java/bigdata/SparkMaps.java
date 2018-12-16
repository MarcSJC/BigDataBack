package bigdata;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.commons.io.FilenameUtils;

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
	
	private static String dataLineToString(int[] dataLine) {
		StringBuffer buffer = new StringBuffer();
		for (int i = 0 ; i < dataLine.length ; ++i) {
			buffer.append(dataLine[i] - minh);
			buffer.append(' ');
		}
		return buffer.toString();
	}

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SparkMaps");
		JavaSparkContext context = new JavaSparkContext(conf);
		int data[][] = new int[dem3Size][dem3Size];
		JavaRDD<byte[]> rdd;
		String filePath = args[0];
		rdd = context.binaryRecords(filePath, 1);
		rdd = rdd.repartition(4);
		byte buffer[] = new byte[2];
		double lat, lng;
		String s = filePath.substring(filePath.length() - 11, filePath.length());
		lat = Double.parseDouble(s.substring(1, 3));
		lng = Double.parseDouble(s.substring(4, 7));
		if (filePath.charAt(0) == 'S' || filePath.charAt(0) == 's') lat *= -1;
        if (filePath.charAt(3) == 'W' || filePath.charAt(3) == 'w') lng *= -1;
		System.out.println(">>>>>>>>>>>>>>>>>>> lat, lng : " + lat + ", " + lng);
		int id = 0;
		int i = 0;
		int j = 0;
		System.out.println(">>>>>>>>>>>>>>>>>>> rdd count : " + rdd.count());
		for (byte[] b : rdd.collect()) {
			if (i >= dem3Size) {
				break;
			}
			if (i == 0) {
				System.out.println(">>>>>>>>>>>>>>>>>>> b length : " + b.length);
			}
			/*int value= 0;
			value = b[1] & 0xFF; // (b[0] << 8) | b[1];
			value = Integer.min(value, maxh);
			value = Integer.max(value, minh);
			minh = Integer.min(value, minh);
			data[i][j] = value;*/
			int value = 0;
			value = b[0] & 0xFF;
			value = Integer.min(value, maxh);
			//value = Integer.max(value, minh);
			minh = Integer.min(value, minh);
			maxh -= minh;
			data[i][j] = value;
			if (j >= dem3Size - 1) {
				i++;
				j = 0;
			}
			else {
				j++;
			}
		}
		//System.out.println(">>>>>>>>>>>>>>>>>>>> data : " + Arrays.deepToString(data));
		PrintWriter writer;
		try {
			//File f = new File(FilenameUtils.removeExtension(filePath) + ".pgm");
			File f = new File("test.pgm");
			writer = new PrintWriter(f, "UTF-8");
			writer.println("P2");
			writer.println(dem3Size + " " + dem3Size);
			writer.println(maxh);
			for (int k = 0 ; k < data.length ; ++k) {
				writer.println(dataLineToString(data[k]));
			}
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}	

	
}
