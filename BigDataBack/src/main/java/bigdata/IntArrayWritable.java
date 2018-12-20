package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/*public class IntArrayWritable extends ArrayWritable {

	public IntArrayWritable(IntWritable[] values) {
		super(IntWritable.class, values);
	}
	
	private static IntWritable[] toIntWritable(int[] array) {
		IntWritable[] values = new IntWritable[array.length];
		for (int i = 0 ; i < array.length ; i++) {
			values[i] = new IntWritable(array[i]);
		}
		return values;
	}
	
	public IntArrayWritable(int[] array) {
		super(IntWritable.class, toIntWritable(array));
	}
	
}*/

public class IntArrayWritable implements Writable {
	private int[] array;
	public IntArrayWritable() {}
	public IntArrayWritable(int[] array) {
		this.array = array;
	}
	public int[] getArray() {
		return array;
	}
	public void setArray(int[] array) {
		this.array = array;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		int length = in.readInt();
        array = new int[length];
        for(int i = 0 ; i < length ; i++) {
            array[i] = in.readInt();
        }
	}
	@Override
	public void write(DataOutput out) throws IOException {
		int length = 0;
        if(array != null) {
            length = array.length;
        }
        out.writeInt(length);
        for(int i = 0 ; i < length ; i++) {
            out.writeInt(array[i]);
        }
	}
}
