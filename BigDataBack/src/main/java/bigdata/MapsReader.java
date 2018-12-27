package bigdata;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Image;
import java.util.List;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple4;

public class MapsReader {
	
	private final static int dem3Size = 1201;
	private final static int tileSize = 256;
	private static final Color SEA_BLUE = new Color(19455);
	private static final Color SNOW_WHITE = new Color(8421416);
	private static short minh = 0;
	private static short maxh = 9000;
	private static int zoom = 7;
	private static int latStep = 90;
	private static int lngStep = 180;
	
	private static int colorGradient(double pred, double pgreen, double pblue) {
		int r = (int) (SEA_BLUE.getRed() * pred + SNOW_WHITE.getRed() * (1 - pred));
		int g = (int) (SEA_BLUE.getGreen() * pgreen + SNOW_WHITE.getGreen() * (1 - pgreen));
		int b = (int) (SEA_BLUE.getBlue() * pblue + SNOW_WHITE.getBlue() * (1 - pblue));
		return new Color(r, g, b).getRGB();
	}
	
	private static int getIntFromColor(int i) {
		int red, green, blue;
		red = (byte) (i & 0xFF);
		green = (byte) ((i >> 8) & 0xFF);
		blue = (byte) ((i >> 16) & 0xFF);
		int rgb;
		if (i <= minh) {
			rgb = SEA_BLUE.getRGB();
		}
		else {
			double pred = Double.max(0, (Math.log(red) / Math.log(maxh)));
			double pgreen = Double.max(0, (Math.log(green) / Math.log(maxh)));
			double pblue = Double.max(0, (Math.log(blue) / Math.log(maxh)));
			rgb = colorGradient(pred, pgreen, pblue);
		}
	    return rgb;
	}
	
	private static int[] getIntArrayRGB(int[] orig) {
		int[] res = new int[orig.length];
		for (int i = 0 ; i < orig.length ; i++) {
			res[i] = getIntFromColor(orig[i]);
		}
		return res;
	}
	
	private static BufferedImage resize(BufferedImage img, int newW, int newH) {
		Image tmp = img.getScaledInstance(newW, newH, Image.SCALE_SMOOTH);
		BufferedImage dimg = new BufferedImage(newW, newH, BufferedImage.TYPE_INT_ARGB);
		Graphics2D g2d = dimg.createGraphics();
		g2d.drawImage(tmp, 0, 0, null);
		g2d.dispose();
		return dimg;
	}
	
	private static BufferedImage intToImg(int[] pxls){
	    BufferedImage outputImage = new BufferedImage(dem3Size, dem3Size, BufferedImage.TYPE_INT_RGB);
		outputImage.setRGB(0, 0, dem3Size, dem3Size, getIntArrayRGB(pxls), 0, dem3Size);
		outputImage = resize(outputImage, tileSize, tileSize);
		return outputImage;
	}
	
	private static void saveImg(BufferedImage img, String path) {
		try {
			ImageIO.write(img, "png", new File(path));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/*private static void saveAllImages(JavaPairRDD<String, scala.Tuple4<Integer, Integer, String, BufferedImage>> r) {
		r.foreach((scala.Tuple2<String, scala.Tuple4<Integer, Integer, String, BufferedImage>> t) -> {
			String imgpath = t._2._3();
			BufferedImage img = t._2._4();
			saveImg(img, imgpath);
		});
	}*/
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SparkMaps");
		JavaSparkContext context = new JavaSparkContext(conf);
		// ----------- Test lecture -----------
		String rawpath = args[0];
		JavaPairRDD<Text, IntArrayWritable> rddRaw = context.sequenceFile(rawpath, Text.class, IntArrayWritable.class);
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> rdd3 : " + rddRaw.count());
		JavaPairRDD<String, int[]> rdd = rddRaw.mapToPair((scala.Tuple2<Text, IntArrayWritable> t) -> {
			String newKey = t._1.toString();
			int[] newVal = t._2.getArray();
			scala.Tuple2<String, int[]> res = new scala.Tuple2<String, int[]>(newKey, newVal);
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> key : " + newKey);
			return res;
		});
		//
		BufferedImage def = new BufferedImage(256, 256, BufferedImage.TYPE_INT_RGB);
		Graphics2D g2d = def.createGraphics();
		g2d.setColor(SEA_BLUE);
		g2d.fillRect(0, 0, tileSize, tileSize);
		//
		JavaPairRDD<String, scala.Tuple4<Integer, Integer, String, BufferedImage>> rddBaseZoom = rdd.mapToPair((scala.Tuple2<String, int[]> t) -> {
			// --- Coordinates ---
			String s = t._1.substring(t._1.length() - 11, t._1.length());
			int lat, lng;
			lat = Integer.parseInt(s.substring(1, 3)) + latStep;
			lng = Integer.parseInt(s.substring(4, 7)) + lngStep;
			String skey = (lat - (lat % 2)) + "," + (lng - (lng % 2));
	        // --- Test tiles ---
	        String parentpath = args[1] + "/testtiles/" + zoom + "/" + lat + "/" + lng + ".png";
	        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> PATH (" + lat + ", " + lng + ") : " + parentpath);
	        File parentDir = new File(parentpath).getParentFile();
	        if (parentDir != null && !parentDir.exists()) {
	        	if (!parentDir.mkdirs()) {
	        		try {
	        			throw new IOException("error creating directories");
	        		} catch (IOException e) {
	        			e.printStackTrace();
	        		}
	        	}
	        }
	        // --- Get image ---
			BufferedImage img = intToImg(t._2);
			//saveImg(img, parentpath);
			// --- Return ---
			scala.Tuple4<Integer, Integer, String, BufferedImage> tuple = new scala.Tuple4<Integer, Integer, String, BufferedImage>(lat, lng, parentpath, img);
			return new scala.Tuple2<String, scala.Tuple4<Integer, Integer, String, BufferedImage>>(skey, tuple);
		});
		// --- Save as image ---
		//saveAllImages(rddBaseZoom);
		rddBaseZoom.foreach((scala.Tuple2<String, scala.Tuple4<Integer, Integer, String, BufferedImage>> t) -> {
			String imgpath = t._2._3();
			BufferedImage img = t._2._4();
			saveImg(img, imgpath);
		});
		//
		zoom--;
		latStep /= 2;
		lngStep /= 2;
		//
		JavaPairRDD<String, Iterable<Tuple4<Integer, Integer, String, BufferedImage>>> rddGby = rddBaseZoom.groupByKey();
		JavaPairRDD<String, scala.Tuple4<Integer, Integer, String, BufferedImage>> rddzm1 = rddGby.mapToPair((scala.Tuple2<String, Iterable<scala.Tuple4<Integer, Integer, String, BufferedImage>>> it) -> {
			String skey = it._1;
			BufferedImage img = new BufferedImage(tileSize * 2, tileSize * 2, BufferedImage.TYPE_INT_RGB);
			Graphics2D g = img.createGraphics();
			int latmin = dem3Size;
			int lngmin = dem3Size;
			for (scala.Tuple4<Integer, Integer, String, BufferedImage> t : it._2) {
				latmin = Integer.min(latmin, t._1());
				lngmin = Integer.min(lngmin, t._2());
				BufferedImage imgpart = t._4();
				int sx = t._1() % 2;
				int sy = t._2() % 2;
				g.drawImage(imgpart, sx * tileSize, sy * tileSize, (sx + 1) * tileSize, (sy + 1) * tileSize, null);
				sx += tileSize;
				sy += tileSize;
			}
			
			// --- Coordinates ---
			int lat, lng;
			lat = latmin / 2;
			lng = lngmin / 2;
	        // --- Test tiles ---
	        String parentpath = args[1] + "/testtiles/" + zoom + "/" + lat + "/" + lng + ".png";
	        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> PATH (" + lat + ", " + lng + ") : " + parentpath);
	        File parentDir = new File(parentpath).getParentFile();
	        if (parentDir != null && !parentDir.exists()) {
	        	if (!parentDir.mkdirs()) {
	        		try {
	        			throw new IOException("error creating directories");
	        		} catch (IOException e) {
	        			e.printStackTrace();
	        		}
	        	}
	        }
			// --- Return ---
			scala.Tuple4<Integer, Integer, String, BufferedImage> tuple = new scala.Tuple4<Integer, Integer, String, BufferedImage>(lat, lng, parentpath, img);
			return new scala.Tuple2<String, scala.Tuple4<Integer, Integer, String, BufferedImage>>(skey, tuple);
		});
		// --- Save as image ---
		//saveAllImages(rddzm1);
		rddzm1.foreach((scala.Tuple2<String, scala.Tuple4<Integer, Integer, String, BufferedImage>> t) -> {
			String imgpath = t._2._3();
			BufferedImage img = t._2._4();
			saveImg(img, imgpath);
		});
		context.close();
	}

}
