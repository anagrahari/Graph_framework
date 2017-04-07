package hadoopComponents;

import graph_filter.FrameWorkMain;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;

public class HadoopJob {
	Integer numMappers = 1;  //Default Value 1
	Integer numReducers =1;  //Default Value 1
	Class <? extends Reducer> clsReducer = hadoopReducer.class;
	Class <? extends Mapper> clsMapper = hadoopMapper.class;
	Class <?> clsOutputKey;
	Class<?> clsOutputValue;
	Class<?> clsTermination = HadoopTerminator.class;
	Job job;
	String jobName;
	
	public void setNumMappers(Integer numMappers)
	{
		this.numMappers = numMappers;
	}
	public Integer getNumReducers(){
		return numReducers;
	}

	public void setNumReducers(Integer numReducers)
	{
		this.numReducers = numReducers;
	}
	public Integer getNumMappers(){
		return numMappers;
	}
	
	public void setmapperClass(Class <? extends Mapper> clsMapper){
		this.clsMapper = clsMapper;
	}
	public Class <? extends Mapper> getMapperClass(){
		return clsMapper;
	}
	
	public void setReducerClass(Class <? extends Reducer> clsReducer){
		this.clsReducer = clsReducer;
	}
	public Class <? extends Reducer> getReducerClass(){
		return clsReducer;
	}

	public void setOtputKeyClass(Class <?> clsOutputKey){
		this.clsOutputKey = clsOutputKey;
	}
	public Class <?> getOtputKeyClass(){
		return clsOutputKey;
	}

	public void setOutputValueClass(Class <?> clsOutputValue){
		this.clsOutputValue = clsOutputValue;
	}
	public Class <?> getoOtputValueClass(){
		return clsOutputValue;
	}

	public void setTerminatorClass(Class <?> clsTermination){
		this.clsTermination = clsTermination;
	}
	public Class <?> getTerminatorClass(){
		return clsTermination;
	}
	
	public String getJobName(){
		return jobName;
	}

	public void setJobName(String jobName)
	{
		this.jobName = jobName;
	}
	
	public Job getJob(){
		return job;
	}
		
	public Job createJob(String jobName) throws IOException {
		Configuration conf = new Configuration();
		this.jobName = jobName;
		job = Job.getInstance(conf, jobName); // jobname
		job.setJarByClass(FrameWorkMain.class);
		job.setMapperClass(clsMapper);
		job.setReducerClass(clsReducer);
		job.setOutputKeyClass(clsOutputKey);
		job.setOutputValueClass(clsOutputValue);
		
//		job.setNumMapTasks(numMappers); 
		job.setNumReduceTasks(numReducers);
		return job;
	}
}
