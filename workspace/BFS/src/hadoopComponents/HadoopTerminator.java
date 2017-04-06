package hadoopComponents;


import org.apache.hadoop.mapreduce.Counters;


public class HadoopTerminator {
	Integer maxIteration = Integer.MAX_VALUE;
	protected Integer iterationCount = 0; 
	void setMaxIteration(Integer maxIteration)
	{
		this.maxIteration = maxIteration;
	}
	
	public void increaseCounter()
	{
		if(iterationCount < maxIteration)
			iterationCount++;
	}
	
	public void resetCounter()
	{
		iterationCount = 0;
	}
	/*
	 * Override this function for different defination of termination condition
	 */
	protected boolean keepGoing()
	{
		if(iterationCount < maxIteration)
		{
			return true;
		}
		return false;
	}
	
	public void updateParams(HadoopJob hJob) throws Exception
	{
	
	}
	
	public String getInputPath(HadoopJob hJob)
	{
		String inputPath = "";
		inputPath = "input/"+hJob.getJobName();
		return inputPath;
	}
	
	public String getOutputtPath(HadoopJob hJob)
	{
		String outputPath = "";
		outputPath = "output/"+hJob.getJobName();
		return outputPath;
	}

}