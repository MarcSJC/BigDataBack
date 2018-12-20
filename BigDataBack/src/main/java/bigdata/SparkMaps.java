package bigdata;

import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import javax.imageio.ImageIO;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableName;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.rdd.SequenceFileRDDFunctions;

public class SparkMaps {
	
	final static int dem3Size= 1201;
	static short minh = 0;
	static short maxh = 255;
	
	private static void intToImg(int[] pxls, String path){
	    BufferedImage outputImage = new BufferedImage(dem3Size, dem3Size, BufferedImage.TYPE_BYTE_GRAY);
		WritableRaster raster = outputImage.getRaster();
		raster.setSamples(0, 0, dem3Size, dem3Size, 0, pxls);
		try {
			ImageIO.write(outputImage, "png", new File(path));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SparkMaps");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaPairRDD<String, PortableDataStream> rdd;
		String filePath = args[0];
		rdd = context.binaryFiles(filePath);
		JavaPairRDD<Text, IntArrayWritable> rdd2 = rdd.mapToPair((scala.Tuple2<String, PortableDataStream> t) -> {
			byte[] arr = t._2.toArray();
			/*String s = path.substring(path.length() - 11, path.length());
			double lat, lng;
			lat = Double.parseDouble(s.substring(1, 3));
			lng = Double.parseDouble(s.substring(4, 7));
			if (s.charAt(0) == 'S' || s.charAt(0) == 's') lat *= -1;
	        if (s.charAt(3) == 'W' || s.charAt(3) == 'w') lng *= -1;*/
			int i = 0;
			int j = 0;
			int data[] = new int[dem3Size * dem3Size];
			for (int k = 0 ; k < arr.length ; k+=2) {
				if (i < dem3Size) {
					byte[] buffer = new byte[2];
					buffer[0] = arr[k];
					buffer[1] = arr[k+1];
					short value = 0;
					//-------------
					ByteBuffer buf = ByteBuffer.wrap(buffer);
					value = buf.getShort();
					//-------------
					if (value < 0) value += 256;
					if (value > 255) value = maxh;
					data[i * dem3Size + j] = value;
					if (j >= dem3Size - 1) {
						i++;
						j = 0;
					}
					else {
						j++;
					}
				}
			}
			Text newKey = new Text(t._1);
			IntArrayWritable newVal = new IntArrayWritable(data);
			scala.Tuple2<Text, IntArrayWritable> res = new scala.Tuple2<Text, IntArrayWritable>(newKey, newVal);
			return res;
		});
		rdd2.saveAsHadoopFile(args[1], Text.class, IntArrayWritable.class, SequenceFileOutputFormat.class, BZip2Codec.class);
		context.close();
	}	
	
	
}
