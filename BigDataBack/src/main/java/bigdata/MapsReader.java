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
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;

public class MapsReader {
	
	private final static int dem3Size = 1201;
	private final static int tileSize = 256;
	private static final Color SEA_BLUE = new Color(19455);
	private static final Color SNOW_WHITE = new Color(242, 242, 242);
	private static final Color MOUNTAIN_ROCK = new Color(115, 77, 38);
	private static final Color GRASS = new Color(0, 179, 0);
	private static final Color SWAMP = new Color(119, 119, 60);
	private static final Color CAPPUCCINO = new Color(204, 153, 102);
	private static short minh = 0;
	private static short maxh = 9000;
	private static int zoom = 8;
	private final static double degreePerBaseTile = 360.0 / 512.0;
		
	private static int colorGradient(int value, double pred, double pgreen, double pblue) {
		Color col1, col2;
		if (value > 3500) {
			col1 = CAPPUCCINO;
			col2 = SNOW_WHITE;
		}
		else if (value > 2000) {
			col1 = MOUNTAIN_ROCK;
			col2 = CAPPUCCINO;
		}
		else if (value > 600) {
			col1 = SWAMP;
			col2 = MOUNTAIN_ROCK;
		}
		else if (value > 200) {
			col1 = GRASS;
			col2 = SWAMP;
		}
		else {
			col1 = SEA_BLUE;
			col2 = GRASS;
		}
		int r = (int) (col1.getRed() * pred + col2.getRed() * (1 - pred));
		int g = (int) (col1.getGreen() * pgreen + col2.getGreen() * (1 - pgreen));
		int b = (int) (col1.getBlue() * pblue + col2.getBlue() * (1 - pblue));
		return new Color(r, g, b).getRGB();
	}
	
	private static int getIntFromColor(int i) {
		double red, green, blue;
		blue = (byte) (i & 0xFF);
		green = (byte) ((i >> 8) & 0xFF);
		red = (byte) ((i >> 16) & 0xFF);
		int rgb;
		if (i <= minh) {
			rgb = SEA_BLUE.getRGB();
		}
		else {
			double pred = Double.max(0, ((i) / (maxh)));
			double pgreen = Double.max(0, ((i) / (maxh)));
			double pblue = Double.max(0, ((i) / (maxh)));
			rgb = colorGradient(i, pred, pgreen, pblue);
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
	
	private static ImageIcon combine4Img(ImageIcon[] imgs) {
		int w = 2 * tileSize;
		BufferedImage image = new BufferedImage(w, w, BufferedImage.TYPE_INT_RGB);
		Graphics2D g2 = image.createGraphics();
		for (int i = 0 ; i < 4 ; i++) {
			if (imgs[i] == null) {
				BufferedImage bImg = new BufferedImage(tileSize, tileSize, BufferedImage.TYPE_INT_RGB);
				Graphics2D graphics = bImg.createGraphics();
				graphics.setPaint(SEA_BLUE);
				graphics.fillRect(0, 0, bImg.getWidth(), bImg.getHeight());

				ImageIcon ocean = new ImageIcon(bImg);
				imgs[i] = ocean;
			}
		}
		g2.drawImage(imgs[0].getImage(), 0, 0, null);
		g2.drawImage(imgs[1].getImage(), tileSize, 0, null);
		g2.drawImage(imgs[2].getImage(), 0, tileSize, null);
		g2.drawImage(imgs[3].getImage(), tileSize, tileSize, null);
		g2.dispose();
		return new ImageIcon(resize(image, tileSize, tileSize));
	}
	
	private static void saveImg(BufferedImage img, String path) {
		try {
			Files.createDirectories(Paths.get(path).getParent());
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		File f = new File(path+".png");
		try {
			ImageIO.write(img, "png", f);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/*private static void saveAllImages(JavaPairRDD<String, ImageIcon> rddzm8, String dirpath) {
		rddzm8.foreach((Tuple2<String, ImageIcon> t) -> {
			String imgpath = dirpath + "/testtiles/" + zoom + "/" + t._1 + ".png";
			BufferedImage img = toBufferedImage(t._2);
			saveImg(img, imgpath);
		});
	}*/
	
	/*private static void insertTile(String strpos, BufferedImage img) throws IOException {
		Configuration config = HBaseConfiguration.create();	
		Connection connection = ConnectionFactory.createConnection(config);
		Table table = connection.getTable(TABLENAME);
		//------------
		String pos = zoom + "/" + strpos;
		Put p = new Put(Bytes.toBytes(pos));
		p.addColumn(Bytes.toBytes("Position"),
				Bytes.toBytes("Path"), Bytes.toBytes(pos));
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ImageIO.write(img, "png", baos);
		p.addColumn(Bytes.toBytes("File"),
				Bytes.toBytes("Tile"), baos.toByteArray());
		baos.close();
		table.put(p);
	}
	
	private static void saveAllToHBase(JavaPairRDD<String, ImageIcon> rddzm8) throws IOException {
		Configuration config = HBaseConfiguration.create();	
		HTableDescriptor hTable = new HTableDescriptor(TABLENAME);
		Connection connection = ConnectionFactory.createConnection(config);
		Admin admin = connection.getAdmin();
		HColumnDescriptor position = new HColumnDescriptor("Position");
		HColumnDescriptor file = new HColumnDescriptor("File");
		hTable.addFamily(position);
		hTable.addFamily(file);
		if (admin.tableExists(hTable.getTableName())) {
			admin.disableTable(hTable.getTableName());
			admin.deleteTable(hTable.getTableName());
		}
		admin.createTable(hTable);
		admin.close();

		rddzm8.foreach((Tuple2<String, ImageIcon> t) -> {
			BufferedImage img = toBufferedImage(t._2);
			insertTile(t._1, img);
		});
	}*/
	
	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf().setAppName("SparkMaps");
		JavaSparkContext context = new JavaSparkContext(conf);
		context.setLogLevel("WARN");
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
		JavaPairRDD<String, int[]> rddzm8Cut = rdd.flatMapToPair((Tuple2<Tuple2<Integer, Integer>, int[]> t) -> {
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
				Tuple2<String, int[]> item = new Tuple2<String, int[]>(zoom + "/" + key, tilePart);
				list.add(item);
			}
			// --- Return ---
			return list.iterator();
		}).cache();
		rddRaw.unpersist();
		
		JavaPairRDD<String, Iterable<int[]>> rddzm8CutGrouped = rddzm8Cut.groupByKey().cache();
	
		rddzm8Cut.unpersist();
		JavaPairRDD<String, ImageIcon> rddzm8 = rddzm8CutGrouped.mapToPair((Tuple2<String, Iterable<int[]>> t) -> {
			int size = (int) (degreePerBaseTile * (double) dem3Size);
			Iterator<int[]> it = t._2.iterator();
			int[] tile = it.next();
			while (it.hasNext()) {
				tile = aggregateIntArrays(tile, it.next());
			}
			ImageIcon img = intToImg(tile, size);
			return new Tuple2<String, ImageIcon>(t._1, img);
		}).cache();
		rddzm8CutGrouped.unpersist();
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> FIN DU rddzm8");
		
		// --- Save to hbase --
		rddzm8.foreach((Tuple2<String, ImageIcon> t) -> {
			//ToolRunner.run(HBaseConfiguration.create(), new HBaseLink.HBaseProg(), null);
			BufferedImage img = toBufferedImage(t._2);
			/*ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ImageIO.write(img, "png", baos);
			HBaseLink.HBaseProg.put(t._1, baos.toByteArray());*/
			// TEST LOCAL
			saveImg(img, args[1]+"/"+t._1);
		});
		
		JavaPairRDD<Tuple3<Integer, Integer, Integer>, Tuple2<Tuple3<Integer, Integer, Integer>, ImageIcon>> rddzm8Keyed = rddzm8.mapToPair((Tuple2<String, ImageIcon> t) -> {
			String[] tokens = t._1.split("/");
			int z = Integer.parseInt(tokens[0]);
			int x = Integer.parseInt(tokens[1]);
			int y = Integer.parseInt(tokens[2]);
			Tuple3<Integer, Integer, Integer> oldKey = new Tuple3<Integer, Integer, Integer>(z, x, y);
			if (x % 2 != 0) {
				x--;
			}
			if (y % 2 != 0) {
				y--;
			}
			Tuple3<Integer, Integer, Integer> newKey = new Tuple3<Integer, Integer, Integer>(z, x, y);
			Tuple2<Tuple3<Integer, Integer, Integer>, ImageIcon> newVal = new Tuple2<Tuple3<Integer, Integer, Integer>, ImageIcon>(oldKey, t._2);
			Tuple2<Tuple3<Integer, Integer, Integer>, Tuple2<Tuple3<Integer, Integer, Integer>, ImageIcon>> res = new Tuple2<Tuple3<Integer, Integer, Integer>, Tuple2<Tuple3<Integer, Integer, Integer>, ImageIcon>>(newKey, newVal);
			return res;
		});
		rddzm8.unpersist();
		
		JavaPairRDD<Tuple3<Integer, Integer, Integer>, Iterable<Tuple2<Tuple3<Integer, Integer, Integer>, ImageIcon>>> rddzm8Grouped = rddzm8Keyed.groupByKey().cache();
		rddzm8Keyed.unpersist();
		
		JavaPairRDD<Tuple3<Integer, Integer, Integer>, Tuple2<Tuple3<Integer, Integer, Integer>, ImageIcon>> rddzm7 = rddzm8Grouped.flatMapToPair((Tuple2<Tuple3<Integer, Integer, Integer>, Iterable<Tuple2<Tuple3<Integer, Integer, Integer>, ImageIcon>>> t) -> {
			ArrayList<Tuple2<Tuple3<Integer, Integer, Integer>, Tuple2<Tuple3<Integer, Integer, Integer>, ImageIcon>>> list = new ArrayList<Tuple2<Tuple3<Integer, Integer, Integer>, Tuple2<Tuple3<Integer, Integer, Integer>, ImageIcon>>>();
			int z = t._1._1() - 1;
			int x = t._1._2();
			int y = t._1._3();
			if (x % 2 != 0) {
				x--;
			}
			if (y % 2 != 0) {
				y--;
			}
			x /= 2;
			y /= 2;
			int kx = x;
			int ky = y;
			if (kx % 2 != 0) {
				kx--;
			}
			if (y % 2 != 0) {
				ky--;
			}
			Tuple3<Integer, Integer, Integer> key = new Tuple3<Integer, Integer, Integer>(z, kx, ky);
			Tuple3<Integer, Integer, Integer> valkey = new Tuple3<Integer, Integer, Integer>(z, x, y);
			Iterator<Tuple2<Tuple3<Integer, Integer, Integer>, ImageIcon>> it = t._2.iterator();
			ImageIcon[] imgArr = new ImageIcon[4];
			while (it.hasNext()) {
				Tuple2<Tuple3<Integer, Integer, Integer>, ImageIcon> item = it.next();
				int i = 0;
				int ix = item._1._2();
				int iy = item._1._3();
				if (ix % 2 != 0) {
					i += 1;
				}
				if (iy % 2 != 0) {
					i += 2;
				}
				imgArr[i] = item._2;
			}
			ImageIcon newImg = combine4Img(imgArr);
			Tuple2<Tuple3<Integer, Integer, Integer>, ImageIcon> val = new Tuple2<Tuple3<Integer, Integer, Integer>, ImageIcon>(valkey, newImg);
			Tuple2<Tuple3<Integer, Integer, Integer>, Tuple2<Tuple3<Integer, Integer, Integer>, ImageIcon>> res = new Tuple2<Tuple3<Integer, Integer, Integer>, Tuple2<Tuple3<Integer, Integer, Integer>, ImageIcon>>(key, val);
			list.add(res);
			return list.iterator();
		});
		rddzm8Grouped.unpersist();
		
		rddzm7.foreach((Tuple2<Tuple3<Integer, Integer, Integer>, Tuple2<Tuple3<Integer, Integer, Integer>, ImageIcon>> t) -> {
			//ToolRunner.run(HBaseConfiguration.create(), new HBaseLink.HBaseProg(), null);
			BufferedImage img = toBufferedImage(t._2._2);
			String path = t._2._1._1() + "/" + t._2._1._2() + "/" + t._2._1._3();
			/*ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ImageIO.write(img, "png", baos);
			HBaseLink.HBaseProg.put(path, baos.toByteArray());*/
			// TEST LOCAL
			saveImg(img, args[1]+"/"+path);
		});
		rddzm7.unpersist();
		
		context.close();
	}

}
