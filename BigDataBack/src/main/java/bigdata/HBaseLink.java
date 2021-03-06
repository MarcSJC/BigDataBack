package bigdata;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;


public class HBaseLink {

	public static class HBaseProg extends Configured implements Tool {
		
		protected static TableName TABLE_NAME = TableName.valueOf("PascalTestTiles5");
		private static Connection connection;
		private static Table table;

		public static void put(String row, byte[] arr) throws IOException {
			Put put = new Put(Bytes.toBytes(row));
			put.addColumn(Bytes.toBytes("Position"),
					Bytes.toBytes("Path"), Bytes.toBytes(row));
			put.addColumn(Bytes.toBytes("File"),
					Bytes.toBytes("Tile"), arr);
			table.put(put);
		}
		
		public int run(String[] args) throws IOException {
			if (connection == null)
				connection = ConnectionFactory.createConnection(getConf());
			if (table == null)
				table = connection.getTable(TABLE_NAME);
			return 0;
		}

	}

}


