package bigdata;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.imageio.ImageIO;
import javax.swing.ImageIcon;

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
	private static double zoom = (Math.log(180) / Math.log(2)) + 1;
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
	
	private static String getTileNumber(double lat, double lon, double zoom) {
		double n = Math.pow(2, zoom);
		int xtile = (int) (n * ((lon + 180) / 360));
		int ytile = (int) (n * (1 - (Math.log(Math.tan(lat) + (1 / Math.cos(lat))) / Math.PI)) / 2);
		return ((int) Math.floor(zoom) + "/" + xtile + "/" + ytile);
	}
	
	private static BufferedImage resize(BufferedImage img, int newW, int newH) {
		Image tmp = img.getScaledInstance(newW, newH, Image.SCALE_SMOOTH);
		BufferedImage dimg = new BufferedImage(newW, newH, BufferedImage.TYPE_INT_RGB);
		Graphics2D g2d = dimg.createGraphics();
		g2d.drawImage(tmp, 0, 0, null);
		g2d.dispose();
		return dimg;
	}
	
	private static ImageIcon intToImg(int[] pxls){
	    BufferedImage outputImage = new BufferedImage(dem3Size, dem3Size, BufferedImage.TYPE_INT_RGB);
		outputImage.setRGB(0, 0, dem3Size, dem3Size, getIntArrayRGB(pxls), 0, dem3Size);
		outputImage = resize(outputImage, tileSize, tileSize);
		return new ImageIcon(outputImage);
	}
	
	private static BufferedImage toBufferedImage(ImageIcon icon) {
		BufferedImage bi = new BufferedImage(icon.getIconWidth(), icon.getIconHeight(), BufferedImage.TYPE_INT_RGB);
		Graphics2D g = bi.createGraphics();
		// paint the Icon to the BufferedImage.
		icon.paintIcon(null, g, 0,0);
		g.dispose();
		return bi;
	}
	
	private static void saveImg(BufferedImage img, String path) {
		try {
			Files.createDirectories(Paths.get(path).getParent());
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {
			ImageIO.write(img, "png", new File(path));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void saveAllImages(JavaPairRDD<String, scala.Tuple4<Integer, Integer, String, ImageIcon>> r) {
		r.foreach((scala.Tuple2<String, scala.Tuple4<Integer, Integer, String, ImageIcon>> t) -> {
			String imgpath = t._2._3();
			BufferedImage img = toBufferedImage(t._2._4());
			saveImg(img, imgpath);
		});
	}
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SparkMaps");
		JavaSparkContext context = new JavaSparkContext(conf);
		context.setLogLevel("WARN");
		// ----------- Test lecture -----------
		String rawpath = args[0];
		JavaPairRDD<Text, IntArrayWritable> rddRaw = context.sequenceFile(rawpath, Text.class, IntArrayWritable.class);
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> rdd3 : " + rddRaw.count());
		JavaPairRDD<String, int[]> rdd = rddRaw.mapToPair((scala.Tuple2<Text, IntArrayWritable> t) -> {
			String newKey = t._1.toString();
			int[] newVal = t._2.getArray();
			scala.Tuple2<String, int[]> res = new scala.Tuple2<String, int[]>(newKey, newVal);
			//System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> key : " + newKey);
			return res;
		});
		//
		/*BufferedImage def = new BufferedImage(256, 256, BufferedImage.TYPE_INT_RGB);
		Graphics2D g2d = def.createGraphics();
		g2d.setColor(SEA_BLUE);
		g2d.fillRect(0, 0, tileSize, tileSize);*/
		//
		JavaPairRDD<String, scala.Tuple4<Integer, Integer, String, ImageIcon>> rddBaseZoom = rdd.mapToPair((scala.Tuple2<String, int[]> t) -> {
			// --- Coordinates ---
			String s = t._1.substring(t._1.length() - 11, t._1.length());
			int lat, lng;
			lat = Integer.parseInt(s.substring(1, 3));
			lng = Integer.parseInt(s.substring(4, 7));
			if (s.charAt(0) == 'S' || s.charAt(0) == 's') lat *= -1;
			if (s.charAt(3) == 'W' || s.charAt(3) == 'w') lng *= -1;
			//lat += latStep;
			//lng += lngStep;
			String skey = (lat - (lat % 2)) + "," + (lng - (lng % 2));
			//System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> SKEY : " + skey);
	        // --- Test tiles ---
			String filepath = args[1] + "/testtiles/" + getTileNumber(lat, lng, zoom) + ".png";
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> FILEPATH : " + filepath);
	        // --- Get image ---
			ImageIcon img = intToImg(t._2);
			// --- Return ---
			scala.Tuple4<Integer, Integer, String, ImageIcon> tuple = new scala.Tuple4<Integer, Integer, String, ImageIcon>(lat, lng, filepath, img);
			return new scala.Tuple2<String, scala.Tuple4<Integer, Integer, String, ImageIcon>>(skey, tuple);
		});
		// --- Save as image ---
		saveAllImages(rddBaseZoom);
		//
		zoom = zoom - 1;
		//latStep = (latStep / 2) - (latStep % 2);
		//lngStep = (lngStep / 2) - (lngStep % 2);
		//
		JavaPairRDD<String, Iterable<Tuple4<Integer, Integer, String, ImageIcon>>> rddGby = rddBaseZoom.groupByKey();
		JavaPairRDD<String, scala.Tuple4<Integer, Integer, String, ImageIcon>> rddzm1 = rddGby.mapToPair((scala.Tuple2<String, Iterable<scala.Tuple4<Integer, Integer, String, ImageIcon>>> it) -> {
			String skey = it._1;
			BufferedImage img = new BufferedImage(tileSize * 2, tileSize * 2, BufferedImage.TYPE_INT_RGB);
			Graphics2D g = img.createGraphics();
			g.setBackground(SEA_BLUE);
			g.setColor(SEA_BLUE);
			g.clearRect(0, 0, tileSize * 2, tileSize * 2);
			// --- Coordinates ---
			int latmin = dem3Size;
			int lngmin = dem3Size;
			int itsize = 0;
			for (scala.Tuple4<Integer, Integer, String, ImageIcon> t : it._2) {
				latmin = Integer.min(latmin, t._1());
				lngmin = Integer.min(lngmin, t._2());
				BufferedImage imgpart = toBufferedImage(t._4());
				int sx = Math.abs((t._1() - 1) % 2);
				int sy = Math.abs((t._2() - 1) % 2);
				g.drawImage(imgpart, sx * tileSize, sy * tileSize, (sx + 1) * tileSize, (sy + 1) * tileSize, null);
				itsize++;
			}
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ITERABLE : " + skey + "  SIZE : " + itsize);
	        // --- Test tiles ---
	        String parentpath = args[1] + "/testtiles/" + getTileNumber(latmin, lngmin, zoom) + ".png";
			// --- Return ---
	        ImageIcon resic = new ImageIcon(img);
			scala.Tuple4<Integer, Integer, String, ImageIcon> tuple = new scala.Tuple4<Integer, Integer, String, ImageIcon>(latmin, lngmin, parentpath, resic);
			return new scala.Tuple2<String, scala.Tuple4<Integer, Integer, String, ImageIcon>>(skey, tuple);
		});
		// --- Save as image ---
		saveAllImages(rddzm1);
		context.close();
	}

}
