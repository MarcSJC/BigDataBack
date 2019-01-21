package bigdata;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import javax.imageio.ImageIO;
import javax.swing.ImageIcon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
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
	private static int zoom = 8;
	private final static double degreePerBaseTile = 360.0 / 512.0;
	static TableName TABLENAME = TableName.valueOf("TestTiles");
	//static TableName POSNAME = TableName.valueOf("Position");
	//static TableName FILENAME = TableName.valueOf("File");
		
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
	
	private static int getLatPixels(int lat) {
		int retLat = 90 - lat;
		return retLat * dem3Size;
	}
	
	private static int getLngPixels(int lng) {
		int retLng = 180 + lng;
		return retLng * dem3Size;
	}
	
	private static Tuple2<Integer, Integer> getTileNumber(int lat, int lng) {
		int size = (int) (degreePerBaseTile * ((double) dem3Size));
		int latPx = getLatPixels(lat);
		int lngPx = getLngPixels(lng);
		int y = (int) Math.floor(latPx / size);
		int x = (int) Math.floor(lngPx / size);
		return new Tuple2<Integer, Integer>(y, x);
	}
	
	private static int getTilePixels(int y) {
		int size = (int) (degreePerBaseTile * ((double) dem3Size));
		return y * size;
	}
	
	private static int getYPixels(int lat, int y) {
		int pxLat = getLatPixels(lat);
		int pxY = getTilePixels(y);
		return Math.abs(pxLat - pxY);
	}
	
	private static int getXPixels(int lng, int x) {
		int pxLng = getLngPixels(lng);
		int pxX = getTilePixels(x);
		return Math.abs(pxLng - pxX);
	}
	
	private static int[] getTileFromIntArray(int[] arr, int size, int demLatGap, int demLngGap, int latGap, int lngGap) {
		int[] res = new int[size * size]; // default : all 0
		int limitj, limiti;
		if (latGap != 0) {
			limitj = Math.abs(size - latGap);
		}
		else {
			limitj = dem3Size - demLatGap;
		}
		if (lngGap != 0) {
			limiti = Math.abs(size - lngGap);
		}
		else {
			limiti = dem3Size - demLngGap;
		}
		int jd = 0, id = 0, py = 0, px = 0;
		for (int jarr = 0 ; jarr < limitj ; jarr++) {
			jd = jarr + latGap;
			py = jarr + demLatGap;
			for (int iarr = 0 ; iarr < limiti ; iarr++) {
				id = iarr + lngGap;
				px = iarr + demLngGap;
				if ((py * dem3Size + px) > arr.length - 1 || (jd * size + id) > res.length - 1) {
					break;
				}
				res[jd * size + id] = arr[py * dem3Size + px];
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
	
	/*private static void saveAllImages(JavaPairRDD<String, ImageIcon> rddzm9, String dirpath) {
		rddzm9.foreach((Tuple2<String, ImageIcon> t) -> {
			String imgpath = dirpath + "/testtiles/" + zoom + "/" + t._1 + ".png";
			BufferedImage img = toBufferedImage(t._2);
			saveImg(img, imgpath);
		});
	}*/
	
	private static void insertTile(Table table, String pos, BufferedImage img) throws IOException {
		// instantiate Put class
		Put p = new Put(Bytes.toBytes(pos)); 
		// add values using add() method
		p.addColumn(Bytes.toBytes("Position"),
				Bytes.toBytes("Path"), Bytes.toBytes(pos));
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ImageIO.write(img, "png", baos);
		p.addColumn(Bytes.toBytes("File"),
				Bytes.toBytes("Tile"), baos.toByteArray());
		baos.close();;
		// save the put Instance to the HTable.
		table.put(p);
	}
	
	private static void saveAllToHBase(JavaPairRDD<String, ImageIcon> rddzm9, String dirpath) throws IOException {
		// instantiate Configuration class
		Configuration config = HBaseConfiguration.create();	
		// instantiate HTable class
		HTableDescriptor hTable = new HTableDescriptor(TABLENAME);
		Connection connection = ConnectionFactory.createConnection(config);
		Admin admin = connection.getAdmin();
		HColumnDescriptor position = new HColumnDescriptor("Position");
		HColumnDescriptor file = new HColumnDescriptor("File");
		hTable.addFamily(position);
		hTable.addFamily(file);
		Table table = connection.getTable(TABLENAME);
		admin.close();

		rddzm9.foreach((Tuple2<String, ImageIcon> t) -> {
			BufferedImage img = toBufferedImage(t._2);
			insertTile(table, t._1, img);
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
		JavaPairRDD<Tuple2<Integer, Integer>, int[]> rdd = rddRaw.mapToPair((Tuple2<Text, IntArrayWritable> t) -> {
			String name = t._1.toString();
			int[] newVal = t._2.getArray();
			// --- Coordinates ---
			String s = name.substring(name.length() - 11, name.length());
			int lat, lng;
			lat = Integer.parseInt(s.substring(1, 3));
			lng = Integer.parseInt(s.substring(4, 7));
			if (s.charAt(0) == 'S' || s.charAt(0) == 's') lat *= -1.0;
			if (s.charAt(3) == 'W' || s.charAt(3) == 'w') lng *= -1.0;
			// --- Results ---
			Tuple2<Integer, Integer> latlng = new Tuple2<Integer, Integer>(lat, lng);
			Tuple2<Tuple2<Integer, Integer>, int[]> res = new Tuple2<Tuple2<Integer, Integer>, int[]>(latlng, newVal);
			return res;
		}).cache();
		//
		JavaPairRDD<String, int[]> rddzm9Cut = rdd.flatMapToPair((Tuple2<Tuple2<Integer, Integer>, int[]> t) -> {
			// --- Coordinates ---
			int lat, lng;
			lat = t._1._1;
			lng = t._1._2;
			Tuple2<Integer, Integer> baseKey = getTileNumber(lat, lng);
			Tuple2<Integer, Integer> endKey = getTileNumber(lat - 1, lng + 1);
			int nbTilesLng;
			int nbTilesLat;
			if (Math.abs(baseKey._1 - endKey._1) > 1) nbTilesLat = 3; 
			else nbTilesLat = 2;
			if (Math.abs(endKey._2 - baseKey._2) > 1) nbTilesLng = 3; 
			else nbTilesLng = 2;
			int[] cases = new int[nbTilesLat * nbTilesLng];
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> nbTilesLat, nbTilesLng : " + nbTilesLat + ", " + nbTilesLng);
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
			int latGap, lngGap;
			latGap = getYPixels(lat, baseKey._1);
			lngGap = getXPixels(lng, baseKey._2);
			int size = (int) (degreePerBaseTile * (double) dem3Size);
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Coord : " + lat + ", " + lng);
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> GAPS : " + latGap + ", " + lngGap);
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> (" + nbTilesLat + ", " + nbTilesLng + ") cases : " + Arrays.toString(cases));
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> demLatGap : " + Math.abs(size - latGap) + " ou " + Math.abs( 2 * size - latGap));
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> demLngGap : " + Math.abs(size - lngGap) + " ou " + Math.abs( 2 * size - lngGap));
			ArrayList<Tuple2<String, int[]>> list = new ArrayList<Tuple2<String, int[]>>();
			for (int i : cases) {
				String key;
				int[] tilePart;
				switch(i) {
					case 1 :
						key = (baseKey._2 + 1) + "/" + baseKey._1;
						tilePart = getTileFromIntArray(t._2, size, 0, Math.abs(size - lngGap), latGap, 0);
						break;
					case 2 :
						key = baseKey._2 + "/" + (baseKey._1 + 1);
						tilePart = getTileFromIntArray(t._2, size, Math.abs(size - latGap), 0, 0, lngGap);
						break;
					case 3 :
						key = (baseKey._2 + 1) + "/" + (baseKey._1 + 1);
						tilePart = getTileFromIntArray(t._2, size, Math.abs(size - latGap), Math.abs(size - lngGap), 0, 0);
						break;
					case 4 :
						key = baseKey._2 + "/" + (baseKey._1 + 2);
						tilePart = getTileFromIntArray(t._2, size, Math.abs(2 * size - latGap), 0, 0, lngGap);
						break;
					case 5 :
						key = (baseKey._2 + 2) + "/" + baseKey._1;
						tilePart = getTileFromIntArray(t._2, size, 0, Math.abs(2 * size - lngGap), latGap, 0);
						break;
					case 6 :
						key = (baseKey._2 + 1) + "/" + (baseKey._1 + 2);
						tilePart = getTileFromIntArray(t._2, size, Math.abs(2 * size - latGap), Math.abs(size - lngGap), 0, 0);
						break;
					case 7 :
						key = (baseKey._2 + 2) + "/" + (baseKey._1 + 1);
						tilePart = getTileFromIntArray(t._2, size, Math.abs(size - latGap), Math.abs(2 * size - lngGap), 0, 0);
						break;
					case 8 :
						key = (baseKey._2 + 2) + "/" + (baseKey._1 + 2);
						tilePart = getTileFromIntArray(t._2, size, Math.abs(2 * size - latGap), Math.abs(2 * size - lngGap), 0, 0);
						break;
					default : // 0
						key = baseKey._2 + "/" + baseKey._1;
						tilePart = getTileFromIntArray(t._2, size, 0, 0, latGap, lngGap);
				}
				//key += idimg++;
				Tuple2<String, int[]> item = new Tuple2<String, int[]>(key, tilePart);
				list.add(item);
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> case : " + i + " (" + key + ")");
			}
			// --- Return ---
			return list.iterator();
		}).cache();
		rddRaw.unpersist();
		
		JavaPairRDD<String, Iterable<int[]>> rddzm9CutGrouped = rddzm9Cut.groupByKey();
	
		rddzm9Cut.unpersist();
		JavaPairRDD<String, ImageIcon> rddzm9 = rddzm9CutGrouped.mapToPair((Tuple2<String, Iterable<int[]>> t) -> {
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
		rddzm9CutGrouped.unpersist();
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> FIN DU RDDZM9");
		// --- Save as image ---
		//saveAllImages(rddzm9, args[1]);
		try {
			saveAllToHBase(rddzm9, args[1]);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		rddzm9.unpersist();
		context.close();
	}

}
