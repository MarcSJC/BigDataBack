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
		return new Tuple2<Integer, Integer>(xtile, ytile);
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
	
	private static void saveAllImages(JavaPairRDD<Tuple2<Integer, Integer>, ImageIcon> rddzm9, String dirpath) {
		rddzm9.foreach((Tuple2<Tuple2<Integer, Integer>, ImageIcon> t) -> {
			String imgpath = dirpath + "/testtiles/" + zoom + "/" + t._1._1 + "/" + t._1._2 + ".png";
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> IMGPATH (zoom " + zoom + ") : " + imgpath);
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
		JavaPairRDD<Tuple2<Integer, Integer>, int[]> rddzm9Cut = rdd.flatMapToPair((Tuple2<Tuple2<Double, Double>, int[]> t) -> {
			// --- Coordinates ---
			double lat, lng;
			lat = t._1._1;
			lng = t._1._2;
			int size = (int) (degreePerBaseTile * (double) dem3Size);
			ArrayList<Tuple2<Tuple2<Integer, Integer>, int[]>> list = new ArrayList<Tuple2<Tuple2<Integer, Integer>, int[]>>();
			for (int i = 0 ; i < 4 ; i++) {
				Tuple2<Integer, Integer> baseKey = getTileNumber(lat, lng, zoom);
				Tuple2<Integer, Integer> key;
				int latGap, lngGap, latStep, lngStep;
				int[] tilePart;
				latStep = 1;
				lngStep = 1;
				/*if (lat < 0) latStep = -1;
				if (lng >= 0) lngStep = -1;*/
				latGap = (int) (Math.abs(lat - tile2lat(baseKey._1, zoom)));
				lngGap = (int) (Math.abs(lng - tile2lon(baseKey._2, zoom)));
				switch(i) {
					case 1 :
						//key = getTileNumber(lat + latStep, lng, zoom);
						key = new Tuple2<Integer, Integer>(baseKey._1 + latStep, baseKey._2);
						/*latGap = (int) (Math.abs(lat - tile2lat(key._1, zoom)));
						lngGap = (int) (Math.abs(lng - tile2lon(key._2, zoom)));*/
						tilePart = getTileFromIntArray(t._2, size, (size - latGap), 0, 0, lngGap);
						break;
					case 2 :
						//key = getTileNumber(lat, lng + lngStep, zoom);
						key = new Tuple2<Integer, Integer>(baseKey._1, baseKey._2 + lngStep);
						/*latGap = (int) (Math.abs(lat - tile2lat(key._1, zoom)));
						lngGap = (int) (Math.abs(lng - tile2lon(key._2, zoom)));*/
						tilePart = getTileFromIntArray(t._2, size, 0, (size - lngGap), latGap, 0);
						break;
					case 3 :
						//key = getTileNumber(lat + latStep, lng + lngStep, zoom);
						key = new Tuple2<Integer, Integer>(baseKey._1 + latStep, baseKey._2 + lngStep);
						/*latGap = (int) (Math.abs(lat - tile2lat(key._1, zoom)));
						lngGap = (int) (Math.abs(lng - tile2lon(key._2, zoom)));*/
						tilePart = getTileFromIntArray(t._2, size, (size - latGap), (size - lngGap), 0, 0);
						break;
					default : // 0
						//key = getTileNumber(lat, lng, zoom);
						//key = new Tuple2<Integer, Integer>(baseKey._1, baseKey._2);
						key = baseKey;
						/*latGap = (int) (Math.abs(lat - tile2lat(key._1, zoom)));
						lngGap = (int) (Math.abs(lng - tile2lon(key._2, zoom)));*/
						tilePart = getTileFromIntArray(t._2, size, 0, 0, latGap, lngGap);
				}
				Tuple2<Tuple2<Integer, Integer>, int[]> item = new Tuple2<Tuple2<Integer, Integer>, int[]>(key, tilePart);
				list.add(item);
			}
			// --- Return ---
			return list.iterator();
		}).cache();
		rddRaw.unpersist();
		JavaPairRDD<Tuple2<Integer, Integer>, Iterable<int[]>> rddzm9CutGrouped = rddzm9Cut.groupByKey();
		/*int gbk = 0;
		Iterator<int[]> itgbk = rddzm9CutGrouped.first()._2.iterator();
		while (itgbk.hasNext()) {
			gbk++;
			itgbk.next();
		}
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> GBK : " + gbk);*/
		rddzm9Cut.unpersist();
		JavaPairRDD<Tuple2<Integer, Integer>, ImageIcon> rddzm9 = rddzm9CutGrouped.mapToPair((Tuple2<Tuple2<Integer, Integer>, Iterable<int[]>> t) -> {
			int size = (int) (degreePerBaseTile * (double) dem3Size);
			Iterator<int[]> it = t._2.iterator();
			int[] tile = it.next();
			while (it.hasNext()) {
				tile = aggregateIntArrays(tile, it.next());
			}
			ImageIcon img = intToImg(tile, size);
			return new Tuple2<Tuple2<Integer, Integer>, ImageIcon>(t._1, img);
		});
		rddzm9CutGrouped.unpersist();
		// --- Save as image ---
		saveAllImages(rddzm9, args[1]);
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> FIN DU RDDZM9");
		rddzm9.unpersist();
		context.close();
	}

}
