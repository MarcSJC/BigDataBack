package bigdata;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;

import javax.imageio.ImageIO;
import javax.swing.ImageIcon;

import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class MapsReader {
	
	private final static int dem3Size = 1201;
	private final static int tileSize = 256;
	private static final Color SEA_BLUE = new Color(19455);
	private static final Color SNOW_WHITE = new Color(8421416);
	private static short minh = 0;
	private static short maxh = 9000;
	private static int zoom = 9;
	private final static double degreePerBaseTile = 360.0 / 512.0;
	
	private static int idimg = 0;
	
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
	
	private static Tuple2<Integer, Integer> getTileNumber(double lat, double lon, int zoom) {
		/*int n = (int) Math.pow(2, zoom);
		int xtile = (int) (n * ((lon + 180) / 360));
		int ytile = (int) (n * (1 - (Math.log(Math.tan(lat) + (1 / Math.cos(lat))) / Math.PI)) / 2);
		return new Tuple2<Integer, Integer>(xtile, ytile);*/
		int xtile = (int)Math.floor((lon + 180) / 360 * (1<<zoom)) ;
		int ytile = (int)Math.floor((1 - Math.log(Math.tan(Math.toRadians(lat)) + 1 / Math.cos(Math.toRadians(lat))) / Math.PI) / 2 * (1<<zoom)) ;
		if (xtile < 0)
			xtile = 0;
		if (xtile >= (1<<zoom))
			xtile = ((1<<zoom)-1);
		if (ytile < 0)
			ytile = 0;
		if (ytile >= (1<<zoom))
			ytile = ((1<<zoom)-1);
		return new Tuple2<Integer, Integer>(ytile, xtile);
	}
	
	private static double tile2lon(int x, int z) {
		//return x / Math.pow(2.0, z) * 360.0 - 180;
		int n = (int) Math.pow(2, z);
		return (x / n * 360.0 - 180.0);
	}

	private static double tile2lat(int y, int z) {
		//double n = Math.PI - (2.0 * Math.PI * y) / Math.pow(2.0, z);
		//return Math.toDegrees(Math.atan(Math.sinh(n)));
		int n = (int) Math.pow(2, z);
		double lat_rad = Math.atan(Math.sinh(Math.PI * (1 - 2 * y / n)));
		return (lat_rad * 180.0 / Math.PI);
	}
	
	private static int[] getTileFromIntArray(int[] arr, int size, int demLatGap, int demLngGap, int latGap, int lngGap) {
		int[] res = new int[size * size]; // default : all 0
		/*System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> demLatGap : " + demLatGap);
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> demLngGap : " + demLngGap);
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> latGap : " + latGap);
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> lngGap : " + lngGap);*/
		int limitj, limiti;
		if (demLatGap == 0) {
			limitj = (size - latGap);
		}
		else {
			limitj = (dem3Size - demLatGap);
		}
		if (demLngGap == 0) {
			limiti = (size - lngGap);
		}
		else {
			limiti = (dem3Size - demLngGap);
		}
		for (int jarr = 0 ; jarr < limitj ; jarr++) {
			for (int iarr = 0 ; iarr < limiti ; iarr++) {
				int item = arr[(jarr + demLatGap) * dem3Size + iarr + demLngGap];
				res[(latGap + jarr) * size + lngGap + iarr] = item;
			}
		}
		return res;
	}
	
	private static int[] aggregateIntArrays(int[] a, int[] b) {
		int size = Math.max(a.length, b.length);
		int[] res = new int[size];
		for (int i = 0 ; i < size ; i++) {
			res[i] = Math.max(a[i], b[i]);
		}
		return res;
	}
	
	private static BufferedImage resize(BufferedImage img, int newW, int newH) {
		Image tmp = img.getScaledInstance(newW, newH, Image.SCALE_SMOOTH);
		BufferedImage dimg = new BufferedImage(newW, newH, BufferedImage.TYPE_INT_RGB);
		Graphics2D g2d = dimg.createGraphics();
		g2d.drawImage(tmp, 0, 0, null);
		g2d.dispose();
		return dimg;
	}
	
	private static ImageIcon intToImg(int[] pxls, int size){
	    BufferedImage outputImage = new BufferedImage(size, size, BufferedImage.TYPE_INT_RGB);
		outputImage.setRGB(0, 0, size, size, getIntArrayRGB(pxls), 0, size);
		outputImage = resize(outputImage, tileSize, tileSize);
		return new ImageIcon(outputImage);
	}
	
	private static BufferedImage toBufferedImage(ImageIcon icon) {
		BufferedImage bi = new BufferedImage(icon.getIconWidth(), icon.getIconHeight(), BufferedImage.TYPE_INT_RGB);
		Graphics2D g = bi.createGraphics();
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
	
	private static void saveAllImages(JavaPairRDD<String, ImageIcon> rddzm9, String dirpath) {
		rddzm9.foreach((Tuple2<String, ImageIcon> t) -> {
			String imgpath = dirpath + "/testtiles/" + zoom + "/" + t._1 + ".png";
			//System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> IMGPATH (zoom " + zoom + ") : " + imgpath);
			BufferedImage img = toBufferedImage(t._2);
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
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> rddRaw : " + rddRaw.count());
		JavaPairRDD<Tuple2<Double, Double>, int[]> rdd = rddRaw.mapToPair((Tuple2<Text, IntArrayWritable> t) -> {
			String name = t._1.toString();
			int[] newVal = t._2.getArray();
			// --- Coordinates ---
			String s = name.substring(name.length() - 11, name.length());
			double lat, lng;
			lat = Double.parseDouble(s.substring(1, 3));
			lng = Double.parseDouble(s.substring(4, 7));
			if (s.charAt(0) == 'S' || s.charAt(0) == 's') lat *= -1.0;
			if (s.charAt(3) == 'W' || s.charAt(3) == 'w') lng *= -1.0;
			// --- Results ---
			Tuple2<Double, Double> latlng = new Tuple2<Double, Double>(lat, lng);
			Tuple2<Tuple2<Double, Double>, int[]> res = new Tuple2<Tuple2<Double, Double>, int[]>(latlng, newVal);
			return res;
		}).cache();
		//
		JavaPairRDD<String, int[]> rddzm9Cut = rdd.flatMapToPair((Tuple2<Tuple2<Double, Double>, int[]> t) -> {
			// --- Coordinates ---
			double lat, lng;
			lat = t._1._1;
			lng = t._1._2;
			Tuple2<Integer, Integer> baseKey = getTileNumber(lat, lng, zoom);
			Tuple2<Integer, Integer> endKey = getTileNumber(lat + 1, lng + 1, zoom);
			int nbTilesLng;
			int nbTilesLat;
			if (endKey._1 - baseKey._1 > 1) nbTilesLat = 3; 
			else nbTilesLat = 2;
			if (endKey._2 - baseKey._2 > 1) nbTilesLng = 3; 
			else nbTilesLng = 2;
			int[] cases = new int[nbTilesLat * nbTilesLng];
			cases[0] = 0;
			cases[1] = 1;
			cases[2] = 2;
			cases[3] = 3;
			if (nbTilesLat > 2 && nbTilesLng > 2) {
				cases[4] = 4;
				cases[5] = 5;
				cases[6] = 6;
				cases[7] = 7;
				cases[8] = 8;
			}
			else if (nbTilesLat > 2) {
				cases[4] = 4;
				cases[5] = 6;
			}
			else if (nbTilesLng > 2) {
				cases[4] = 5;
				cases[5] = 7;
			}
			int latGap, lngGap, latStep, lngStep;
			latGap = (int) (Math.abs(lat - tile2lat(baseKey._1, zoom)));
			lngGap = (int) (Math.abs(lng - tile2lon(baseKey._2, zoom)));
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> GAPS : " + latGap + ", " + lngGap);
			latStep = 1;
			lngStep = 1;
			int size = (int) (degreePerBaseTile * (double) dem3Size);
			ArrayList<Tuple2<String, int[]>> list = new ArrayList<Tuple2<String, int[]>>();
			for (int i : cases) {
				String key;
				int[] tilePart;
				switch(i) {
					case 1 :
						key = (baseKey._2 + lngStep) + "/" + baseKey._1;
						tilePart = getTileFromIntArray(t._2, size, 0, (size - lngGap), latGap, 0);
						break;
					case 2 :
						key = baseKey._2 + "/" + (baseKey._1 + latStep);
						tilePart = getTileFromIntArray(t._2, size, (size - latGap), 0, 0, lngGap);
						break;
					case 3 :
						key = (baseKey._2 + lngStep) + "/" + (baseKey._1 + latStep);
						tilePart = getTileFromIntArray(t._2, size, (size - latGap), (size - lngGap), 0, 0);
						break;
					case 4 :
						key = baseKey._2 + "/" + (baseKey._1 + 2 * latStep);
						tilePart = getTileFromIntArray(t._2, size, (2 * size - latGap), 0, 0, lngGap);
						break;
					case 5 :
						key = (baseKey._2 + 2 * lngStep) + "/" + baseKey._1;
						tilePart = getTileFromIntArray(t._2, size, 0, (2 * size - lngGap), latGap, 0);
						break;
					case 6 :
						key = (baseKey._2 + lngStep) + "/" + (baseKey._1 + 2 * latStep);
						tilePart = getTileFromIntArray(t._2, size, (2 * size - latGap), (size - lngGap), 0, 0);
						break;
					case 7 :
						key = (baseKey._2 + 2 * lngStep) + "/" + (baseKey._1 + latStep);
						tilePart = getTileFromIntArray(t._2, size, (size - latGap), (2 * size - lngGap), 0, 0);
						break;
					case 8 :
						key = (baseKey._2 + 2 * lngStep) + "/" + (baseKey._1 + 2 * latStep);
						tilePart = getTileFromIntArray(t._2, size, (2 * size - latGap), (2 * size - lngGap), 0, 0);
						break;
					default : // 0
						key = baseKey._2 + "/" + baseKey._1;
						tilePart = getTileFromIntArray(t._2, size, 0, 0, latGap, lngGap);
				}
				key += idimg++;
				Tuple2<String, int[]> item = new Tuple2<String, int[]>(key, tilePart);
				//System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> IMG : (" + lat + ", " + lng + ") : " + key._2 + ", " + key._1);
				list.add(item);
			}
			// --- Return ---
			return list.iterator();
		}).cache();
		rddRaw.unpersist();
		
		JavaPairRDD<String, ImageIcon> rddTest = rddzm9Cut.mapToPair((Tuple2<String, int[]> t) -> {
			int size = (int) (degreePerBaseTile * (double) dem3Size);
			ImageIcon img = intToImg(t._2, size);
			return new Tuple2<String, ImageIcon>(t._1, img);
		});
		saveAllImages(rddTest, args[1] + "/Cut");
		
		//System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> rddzm9Cut : " + rddzm9Cut.count());
		
		JavaPairRDD<String, Iterable<int[]>> rddzm9CutGrouped = rddzm9Cut.groupByKey();
	
		rddzm9Cut.unpersist();
		/*JavaPairRDD<String, ImageIcon> rddzm9 = rddzm9CutGrouped.mapToPair((Tuple2<String, Iterable<int[]>> t) -> {
			int size = (int) (degreePerBaseTile * (double) dem3Size);
			Iterator<int[]> it = t._2.iterator();
			int[] tile = it.next();
			int l = 1;
			while (it.hasNext()) {
				l++;
				tile = aggregateIntArrays(tile, it.next());
			}
			ImageIcon img = intToImg(tile, size);
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> TILE : " + t._1 + "(" + l + ")");
			return new Tuple2<String, ImageIcon>(t._1, img);
		});
		rddzm9CutGrouped.unpersist();*/
		// --- Save as image ---
		//saveAllImages(rddzm9, args[1]);
		//System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> rddzm9 : " + rddzm9.count());
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> FIN DU RDDZM9");
		//rddzm9.unpersist();
		context.close();
	}

}
