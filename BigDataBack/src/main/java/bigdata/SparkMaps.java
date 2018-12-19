package bigdata;

import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import javax.imageio.ImageIO;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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
		int data[] = new int[dem3Size * dem3Size];
		JavaRDD<byte[]> rdd;
		String filePath = args[0];
		rdd = context.binaryRecords(filePath, 2);
		//rdd = rdd.repartition(4);
		rdd.coalesce(4, false);
		double lat, lng;
		String s = filePath.substring(filePath.length() - 11, filePath.length());
		lat = Double.parseDouble(s.substring(1, 3));
		lng = Double.parseDouble(s.substring(4, 7));
		if (filePath.charAt(0) == 'S' || filePath.charAt(0) == 's') lat *= -1;
        if (filePath.charAt(3) == 'W' || filePath.charAt(3) == 'w') lng *= -1;
		System.out.println(">>>>>>>>>>>>>>>>>>> lat, lng : " + lat + ", " + lng);
		int i = 0;
		int j = 0;
		System.out.println(">>>>>>>>>>>>>>>>>>> rdd count : " + rdd.count());
		for (byte[] b : rdd.collect()) {
			if (i < dem3Size) {
				short value = 0;
				//-------------
				ByteBuffer buf = ByteBuffer.wrap(b);
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
		intToImg(data, "mary.png");
	}	

	
}
