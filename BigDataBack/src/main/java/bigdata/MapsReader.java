package bigdata;

import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class MapsReader {
	
	final static int dem3Size = 1201;
	static short minh = 0;
	static short maxh = 255;
	
	private static int getIntFromColor(int i) {
		//int gray = (int) Math.round(255.0 * (i - minh) / (maxh - minh));
		int red, green, blue;
		red = (byte) (i & 0xFF);
		green = (byte) ((i >> 4) & 0xFF);
		blue = (byte) ((i >> 16) & 0xFF);
	    /*red = (gray << 16) & 0x00FF0000;
	    green = (gray << 8) & 0x0000FF00;
	    blue = gray & 0x000000FF;
	    return 0xFF000000 | red | green | blue;*/
		red = (int) (255.0 * (Math.log(red) / Math.log(maxh)));
		green = (int) (255.0 * (Math.log(green) / Math.log(maxh)));
		blue = (int) (255.0 * (Math.log(blue) / Math.log(maxh)));
		int rgb = red;
		rgb = (rgb << 8) + green;
		rgb = (rgb << 8) + blue;
	    return rgb;
	}
	
	private static int[] getIntArrayRGB(int[] orig) {
		int[] res = new int[orig.length];
		for (int i = 0 ; i < orig.length ; i++) {
			res[i] = getIntFromColor(orig[i]);
		}
		return res;
	}
	
	private static void intToImg(int[] pxls, String path){
	    BufferedImage outputImage = new BufferedImage(dem3Size, dem3Size, BufferedImage.TYPE_INT_RGB);
		WritableRaster raster = outputImage.getRaster();
		//raster.setSamples(0, 0, dem3Size, dem3Size, 0, pxls);
		outputImage.setRGB(0, 0, dem3Size, dem3Size, getIntArrayRGB(pxls), 0, dem3Size);
		try {
			ImageIO.write(outputImage, "png", new File(path));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SparkMaps");
		JavaSparkContext context = new JavaSparkContext(conf);
		// ----------- Test lecture -----------
		JavaPairRDD<Text, IntArrayWritable> rdd3 = context.sequenceFile(args[0], Text.class, IntArrayWritable.class);
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> rdd3 : " + rdd3.count());
		JavaPairRDD<String, int[]> rdd4 = rdd3.mapToPair((scala.Tuple2<Text, IntArrayWritable> t) -> {
			String newKey = t._1.toString();
			int[] newVal = t._2.getArray();
			scala.Tuple2<String, int[]> res = new scala.Tuple2<String, int[]>(newKey, newVal);
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> key : " + newKey);
			return res;
		});
		int m = 0;
		for (scala.Tuple2<String, int[]> t : rdd4.collect()) {
			intToImg(t._2, "mary"+m+".png");
			m++;
		}
		context.close();
	}

}
