package cc_mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class ConnectedComponent extends Configured implements Tool {
	
	
	public static class MapperFirstStage extends Mapper<LongWritable, Text, LongWritable, VertexWritable> {
		private static final Log LOG = LogFactory.getLog(MapperFirstStage.class);
		public void map(Long key, Text valueIn, Context con) throws IOException, InterruptedException {
			String line = valueIn.toString();
			String[] vertices =  line.split(",");
			LongWritable vertexId = new LongWritable();
			
			vertexId.set(Long.parseLong(vertices[0]));
			
			Vector <LongWritable> neighbors = new Vector<LongWritable>();
			for (int i = 1; i < vertices.length; i++) {
				LongWritable adj = new LongWritable();
				adj.set(Long.parseLong(vertices[i]));
				neighbors.add(adj);
			}
			
			IntWritable initialId = new IntWritable(0);
			VertexWritable vertex = new VertexWritable(initialId, vertexId, vertexId, neighbors);
			
			LOG.info("Vertex id" + vertexId + "\t Vertex: " + vertex);
			con.write(vertexId, vertex);
		}	
	
	}
	
	public static class MapperSecondStage extends Mapper<LongWritable, VertexWritable, LongWritable, VertexWritable> {
		
		public void map(Long key, VertexWritable inVertex, Context con) throws IOException, InterruptedException {
			con.write(inVertex.vId, inVertex);
			for(LongWritable vertexId : inVertex.neighbors) {
				con.write(vertexId, new VertexWritable(new IntWritable(1), inVertex.group));
			}
		}	
		
	}
	
	public static class ReducerSecondStage extends Reducer<LongWritable, Iterator<VertexWritable>, LongWritable, VertexWritable> {

		public void reduce(LongWritable vertexId, Iterable<VertexWritable> values, Context con) throws IOException, InterruptedException {
			LongWritable M = new LongWritable();
			M.set(Long.MAX_VALUE);
			Object clone = null;
			
			Iterator <VertexWritable> iter = values.iterator();
			
			while(iter.hasNext()) {
				VertexWritable v = iter.next();
				if (v.tag.get() == 0) {
					clone = v.neighbors.clone();					
				}
				M.set(Math.min(M.get(), v.group.get()));
				
//				con.write(M, new VertexWritable(new IntWritable(0), M, vertexId, (Vector<LongWritable>) clone));
			}
			con.write(M, new VertexWritable(new IntWritable(0), M, vertexId, (Vector<LongWritable>) clone));
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
	
	public static void main(String[] args)
	{
		System.out.println("Reached here");
		int res = 0;
		try {
			res = ToolRunner.run(new Configuration(), new ConnectedComponent(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	
		System.exit(res);
	}
	
	public int run(String[] args) throws Exception {
		inputPath = args[0];
		tempPath = args[1];
	    outputPath = args[2];
	    System.out.println(inputPath);
	    firstJob().waitForCompletion(true);
//	    for(int i = 0; i < 5; i++) {
//	    	secondJob(i).waitForCompletion(true);
//	    }
//	    finalJob(5).waitForCompletion(true);
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
		job.setOutputValueClass(VertexWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(tempPath+ "/" + String.valueOf(0) ));
		
	//	job.setNumMapTasks(numMappers); 
		job.setNumReduceTasks(0);
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
		job.setOutputValueClass(VertexWritable.class);
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