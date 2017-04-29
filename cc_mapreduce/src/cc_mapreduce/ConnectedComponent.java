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
	
	public Vertex(int tag, long group) {
		this.tag = tag;
		this.group = group;
	}
	
	int tag;
	long group;
	long vId;
	Vector<Long> neighbors;
}

public class ConnectedComponent {
	public static class MapperFirstStage extends Mapper<Long, Text, Long, Vertex> {
		
		public void map(Long key, Text valueIn, Context con) throws IOException, InterruptedException {
			String line = valueIn.toString();
			String[] vertices =  line.split("");
			long vertexId =  Long.parseLong(vertices[0]);
			
			Vector <Long> neighbors = new Vector<Long>();
			for (int i = 1; i < vertices.length; i++) {
				neighbors.add(Long.parseLong(vertices[i]));
			}
			con.write(vertexId, new Vertex(0, vertexId, vertexId, neighbors));
		}	
	
	}
	
	public static class MapperSecondStage extends Mapper<Long, Vertex, Long, Vertex> {
		
		public void map(Long key, Vertex inVertex, Context con) throws IOException, InterruptedException {
			con.write(inVertex.vId, inVertex);
			for(long vertexId : inVertex.neighbors) {
				con.write(vertexId, new Vertex(1, inVertex.group));
			}
		}	
		
	}
	
	public static class ReducerSecondStage extends Mapper<Long, Vector<Vertex>, Long, Vertex> {
		
		@SuppressWarnings("unchecked")
		public void reduce(Long vertexId, Vector<Vertex> values, Context con) throws IOException, InterruptedException {
			Long M = Long.MAX_VALUE;
			Object clone = null;
			
			for(Vertex v : values) {
				if (v.tag == 0) {
					clone = v.neighbors.clone();					
				}
				M = Math.min(M, v.group);
				//con.write(M, new Vertex(0, M, vertexId, (Vector<Long>) clone));
			}
			con.write(M, new Vertex(0, M, vertexId, (Vector<Long>) clone));
		}
	}
	
	
	public static class MapperFinalStage  extends Mapper <Long, Long, Long, Long> {
		
		public void map(Long group,  Long value, Context con) throws IOException, InterruptedException {
			con.write(group, 1L);
		}
		
	}
	
	public static class ReducerFinalStage extends Reducer<Long, Vector<Long>, Long, Long> {
		
		public void reduce(Long group, Vector<Long> values, Context con) throws IOException, InterruptedException {
			Long m = 0L;
			
			for(Long v : values) {
				m = m + v;
			}
			con.write(group, m);
		}
		
	}
	
}