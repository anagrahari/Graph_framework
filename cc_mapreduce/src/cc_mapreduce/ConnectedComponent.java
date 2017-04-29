package cc_mapreduce;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.Vector;



class Vertex {
	public Vertex(int tag, long group, long vId, Vector<Long> neighbors) {
		// TODO Auto-generated constructor stub
		this.tag = tag;
		this.group = group;
		this.vId = vId;
		this.neighbors = neighbors;
	}
	int tag;
	long group;
	long vId;
	Vector<Long> neighbors;
}

public class ConnectedComponent {
	public static class MapperFirstStage extends Mapper<Long, Text, Long, Vertex> {
		
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String line = value.toString();
			String[] vertices =  line.split("");
			long vertexId =  Long.parseLong(vertices[0]);
			
			Vector <Long> neighbors = new Vector<Long>();
			for (int i = 1; i < vertices.length; i++) {
				neighbors.add(Long.parseLong(vertices[i]));
			}
			con.write(vertexId, new Vertex(0, vertexId, vertexId, neighbors));
		}
		
	
	}
	
	
	
	public static class MapperSecondStage extends Mapper<LongWritable, Text, Text, IntWritable> {
		
	}
	
	public static class ReducerSecondStage extends Mapper<LongWritable, Text, Text, IntWritable> {
		
	}
	
	
	public static class MapperFinalStage  extends Mapper <LongWritable, Text, Text, IntWritable> {
		
	}
	
}