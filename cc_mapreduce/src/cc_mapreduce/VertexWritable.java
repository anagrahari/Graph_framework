package cc_mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class VertexWritable implements Writable {
	IntWritable tag;
	LongWritable group;
	LongWritable vId;
	Vector<LongWritable> neighbors;
	
	public VertexWritable() {
		this.tag = new IntWritable();
		this.group = new LongWritable();
		this.vId = new LongWritable();
		this.neighbors = new Vector<LongWritable>();
	}
	
	public VertexWritable(IntWritable tag, LongWritable group, LongWritable vId, Vector<LongWritable> neighbors) {
		this.tag = tag;
		this.group = group;
		this.vId = vId;
		this.neighbors = neighbors;
	}
	
	public VertexWritable(IntWritable tag, LongWritable group) {
		this.tag = tag;
		this.group = group;
	}
	
	

	public void write(DataOutput out) throws IOException {
		tag.write(out);
		group.write(out);
		vId.write(out);
		for(LongWritable x: neighbors) {
			x.write(out);
		}	
	}

	public void readFields(DataInput in) throws IOException {
		tag.readFields(in);
		group.readFields(in);
		vId.readFields(in);
		for(LongWritable x: neighbors) {
			x.readFields(in);
		}		
	}

}
