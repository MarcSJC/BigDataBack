package bigdata;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class MapsReader {
	
	final static int dem3Size = 1201;
	final static int tileSize = 256;
	private static final Color SEA_BLUE = new Color(19455);
	private static final Color SNOW_WHITE = new Color(8421416);
	static short minh = 0;
	static short maxh = 9000;
	
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
	
	private static void intToImg(int[] pxls, String path){
	    BufferedImage outputImage = new BufferedImage(dem3Size, dem3Size, BufferedImage.TYPE_INT_RGB);
		outputImage.setRGB(0, 0, dem3Size, dem3Size, getIntArrayRGB(pxls), 0, dem3Size);
		outputImage = resize(outputImage, tileSize, tileSize);
		try {
			ImageIO.write(outputImage, "png", new File(path));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/*private static scala.Tuple2<Double, Double> mercator(double lat, double lng, int zoom) {
		double radlng = lng * Math.PI / 180;
		double radlat = lat * Math.PI / 180;
		double x = (256 / (2 * Math.PI)) * (2 ^ zoom) * (radlng + Math.PI);
		double y = (256 / (2 * Math.PI)) * (2 ^ zoom) * (Math.PI - Math.log(Math.tan((Math.PI / 4) + (radlat / 2))));
		return new scala.Tuple2<Double, Double>(x, y);
	}*/
	
	private static String getTileNumber(double lat, double lon, int zoom) {
		int xtile = (int) Math.floor((lon + 180) / 360 * (1<<zoom) ) ;
		int ytile = (int) Math.floor((1 - Math.log(Math.tan(Math.toRadians(lat)) + 1 / Math.cos(Math.toRadians(lat))) / Math.PI) / 2 * (1<<zoom) ) ;
		if (xtile < 0)
			xtile = 0;
		if (xtile >= (1<<zoom))
			xtile = ((1<<zoom) - 1);
		if (ytile < 0)
			ytile = 0;
		if (ytile >= (1<<zoom))
			ytile = ((1<<zoom) - 1);
		/*scala.Tuple2<Double, Double> t = mercator(lat, lon, zoom);
		double xtile = t._1$mcD$sp();
		double ytile = t._2$mcD$sp();*/
		return("testtiles/" + zoom + "/" + xtile + "/" + ytile);
	}
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SparkMaps");
		JavaSparkContext context = new JavaSparkContext(conf);
		// ----------- Test lecture -----------
		String rawpath = args[0];
		JavaPairRDD<Text, IntArrayWritable> rdd3 = context.sequenceFile(rawpath, Text.class, IntArrayWritable.class);
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> rdd3 : " + rdd3.count());
		JavaPairRDD<String, int[]> rdd4 = rdd3.mapToPair((scala.Tuple2<Text, IntArrayWritable> t) -> {
			String newKey = t._1.toString();
			int[] newVal = t._2.getArray();
			scala.Tuple2<String, int[]> res = new scala.Tuple2<String, int[]>(newKey, newVal);
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> key : " + newKey);
			return res;
		});
		for (scala.Tuple2<String, int[]> t : rdd4.collect()) {
			// --- Coordinates ---
			String s = t._1.substring(t._1.length() - 11, t._1.length());
			double lat, lng;
			lat = Double.parseDouble(s.substring(1, 3));
			lng = Double.parseDouble(s.substring(4, 7));
			if (s.charAt(0) == 'S' || s.charAt(0) == 's') lat *= -1;
	        if (s.charAt(3) == 'W' || s.charAt(3) == 'w') lng *= -1;
	        // --- Test tiles ---
	        String output = getTileNumber(lat, lng, 3);
	        String parentpath = args[1] + "/" + output + ".png";
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
	        // --- Save as image ---
			intToImg(t._2, parentpath);
		}
		context.close();
	}

}
