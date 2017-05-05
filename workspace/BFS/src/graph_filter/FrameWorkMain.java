package graph_filter;

import hadoopComponents.HadoopJob;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FrameWorkMain extends Configured implements Tool{
	public HadoopJob hJob;
	@Override
	public int run(String[] arg0) throws Exception {
		String keepGoing = "keepGoing";
		String updateParams = "updateParams";
		String inputPath = "getInputPath";
		String outputPath = "getOutputtPath";
		Object termination;
		Boolean isTerminate;
		Method getNameMethod;
		int iterationCount = 0;
		int numMappers = 1;
		int numReducers = 1;
		Class cls;
		
		Job job;
		FileSystem fs = FileSystem.get(new Configuration());
		Configuration conf = getConf();
		String path="logs/"+hJob.getJobName();
		FileSystem fs1 = FileSystem.get(URI.create(path),conf);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs1.create(new Path(path),true)));
		Calendar startTime = Calendar.getInstance();
		try{
			/*
			 * Reflection to get if termination condition met 
			 */
			cls = hJob.getTerminatorClass();
			termination = cls.newInstance(); // invoke empty constructor
			getNameMethod = cls.getMethod(keepGoing);
			isTerminate = (Boolean)getNameMethod.invoke(termination);
			while (isTerminate.booleanValue()) {
				//If need to set mappers and reducers here
				iterationCount++;
				hJob.setNumReducers(numReducers);
				hJob.setNumMappers(numMappers);
				
				//get Job
				job = hJob.createJob(hJob.getJobName());
				
				//Set Input and Output paths
				getNameMethod = termination.getClass().getMethod(inputPath, HadoopJob.class);
				System.out.println("new path" + new Path((String)getNameMethod.invoke(termination, hJob)));
				FileInputFormat.addInputPath(job, new Path((String)getNameMethod.invoke(termination, hJob)));
				getNameMethod = termination.getClass().getMethod(outputPath, HadoopJob.class);
				System.out.println("name method" + getNameMethod);
				FileOutputFormat.setOutputPath(job, new Path((String)getNameMethod.invoke(termination, hJob)));
				
				//Wait for job completion
				job.waitForCompletion(true);
				
				//Call for update params
				getNameMethod = termination.getClass().getMethod(updateParams, HadoopJob.class);
				isTerminate = (Boolean)getNameMethod.invoke(termination, hJob);
				getNameMethod = cls.getMethod(keepGoing);
				isTerminate = (Boolean)getNameMethod.invoke(termination);
			}
		}catch(Exception e){
			e.printStackTrace();
		} 
		Calendar finishTime = Calendar.getInstance();
		bw.write("Total Iterations: "+iterationCount+"\n");
		bw.write("Time Taken: "+(finishTime.getTimeInMillis()-startTime.getTimeInMillis())/1000.0+" seconds\n");
		bw.close();
		return 0;
	}
	
	public void runMain(String[] args, HadoopJob hJob)
	{
		int res = 0;
		this.hJob = hJob;
		try {
			System.out.println("i ot it");
			res = ToolRunner.run(new Configuration(), this, args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(res);
	}

}
