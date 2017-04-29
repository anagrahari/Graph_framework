package cc_mapreduce;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;



class Vertex {
	public Vertex(int tag, long group, long vId, Vector<Long> neighbors) {
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

public class ConnectedComponent extends Configured implements Tool {
	
	
	public static class MapperFirstStage extends Mapper<Long, Text, Long, Vertex> {
		
		public void map(Long key, Text valueIn, Context con) throws IOException, InterruptedException {
			String line = valueIn.toString();
			String[] vertices =  line.split(",");
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
	
	public static class ReducerSecondStage extends Reducer<Long, Vector<Vertex>, Long, Vertex> {
		
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
	
	protected String inputPath = null;
	protected String tempPath = null;
	protected String outputPath = null;
	protected int numReducers = 1;
	
	public void main(String[] args)
	{
		int res = 0;
		try {
			res = ToolRunner.run(new Configuration(), this, args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(res);
	}
	
	public int run(String[] args) throws Exception {
		inputPath = args[0];
		tempPath = args[1];
	    outputPath = args[2];
	    
	    firstJob().waitForCompletion(true);
	    for(int i = 0; i < 5; i++) {
	    	secondJob(i).waitForCompletion(true);
	    }
	    finalJob(5).waitForCompletion(true);
		return 0;
	}
	
	// Configure stage1
    protected Job  firstJob() throws Exception
    {
    	Configuration conf = new Configuration();		
		Job job = Job.getInstance(conf, "firstStage"); // jobname
		job.setJarByClass(ConnectedComponent.class);
		job.setMapperClass(MapperFirstStage.class);
		job.setOutputKeyClass(Long.class);
		job.setOutputValueClass(Vertex.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(tempPath+ "/" + String.valueOf(0) ));
		
	//	job.setNumMapTasks(numMappers); 
		job.setNumReduceTasks(numReducers);
		return job;
	    
    }

    // Configure stage2
    protected Job secondJob(int i) throws Exception
    {
    	Configuration conf = new Configuration();		
		Job job = Job.getInstance(conf, "secondStage"); // jobname
        
		job.setJarByClass(ConnectedComponent.class);
		job.setMapperClass(MapperSecondStage.class);
		job.setReducerClass(ReducerSecondStage.class);
		job.setOutputKeyClass(Long.class);
		job.setOutputValueClass(Vertex.class);
		FileInputFormat.addInputPath(job, new Path(tempPath + "/" + String.valueOf(i)));
		FileOutputFormat.setOutputPath(job, new Path(tempPath +"/" + String.valueOf(i+1)));
		
	//	job.setNumMapTasks(numMappers); 
		job.setNumReduceTasks(numReducers);
		return job;
    }

	// Configure stage3
    protected Job finalJob (int i) throws Exception
    {
    	Configuration conf = new Configuration();		
		Job job = Job.getInstance(conf, "finalStage"); // jobname
        
		job.setJarByClass(ConnectedComponent.class);
		job.setMapperClass(MapperFinalStage.class);
		job.setReducerClass(ReducerFinalStage.class);
		job.setOutputKeyClass(Long.class);
		job.setOutputValueClass(Long.class);
		FileInputFormat.addInputPath(job, new Path(tempPath + "/" + String.valueOf(i)));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
	//	job.setNumMapTasks(numMappers); 
		job.setNumReduceTasks(numReducers);
		return job;
    }
	
}