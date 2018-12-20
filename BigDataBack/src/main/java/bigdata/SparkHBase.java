package bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkHBase {
	
	public static class HBaseProg extends Configured implements Tool {
		private static final byte[] POSFAMILY = Bytes.toBytes("Position");
		private static final byte[] FILES    = Bytes.toBytes("Files");
		private static final byte[] TABLE_NAME = Bytes.toBytes("Spark_Project");

		public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
			if (admin.tableExists(table.getTableName())) {
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);
		}

		public static void createTable(Connection connect) {
			try {
				final Admin admin = connect.getAdmin(); 
				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
				HColumnDescriptor posFam = new HColumnDescriptor(POSFAMILY); 
				HColumnDescriptor files = new HColumnDescriptor(FILES); 

				tableDescriptor.addFamily(posFam);
				tableDescriptor.addFamily(files);

				createOrOverwrite(admin, tableDescriptor);
				admin.close();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}
		
		public static void insertInTable() {
			Configuration config = HBaseConfiguration.create();
			
			// create Key, Value pair to store in HBase
			/*JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = javaRDD.mapToPair(
					new PairFunction<Row, ImmutableBytesWritable, Put>() {
					@Override
					public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {
			         
			       Put put = new Put(Bytes.toBytes(getpos));
			       put.add(Bytes.toBytes("Position"), Bytes.toBytes("columnQualifier1"), Bytes.toBytes(row.getString(1)));
			       put.add(Bytes.toBytes("Files"), Bytes.toBytes("columnQualifier2"), Bytes.toBytes(row.getString(2)));
			     
			           return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);     
			    }
			     });*/
			     
			     // save to HBase- Spark built-in API method
			    // hbasePuts.saveAsNewAPIHadoopDataset(config.getConfiguration());
			
		}

		@Override
		public int run(String[] arg0) throws Exception {
			Connection connection = ConnectionFactory.createConnection(getConf());
			createTable(connection);
			Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
			Put put = new Put(Bytes.toBytes("KEY"));
			table.put(put);
			return 0;
		}

	}

	/*public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(), new SparkHBase.HBaseProg(), args);
		System.exit(exitCode);
	}*/
}
