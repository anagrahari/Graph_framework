package Graph_Algorithm;

import hadoopComponents.HadoopJob;
import hadoopComponents.HadoopTerminator;

import org.apache.hadoop.mapreduce.Counters;

public class TerminatorClassBFS extends HadoopTerminator{
	long numGrayUnProcessed = 1;
	long numGrayProcessed = 0;
	public boolean keepGoing()
	{
		while (numGrayUnProcessed > 0) {
			return true;
		}
		return false;
	}
	public void updateParams(HadoopJob hJob) throws Exception
	{
		increaseCounter();
		Counters counters = hJob.getJob().getCounters();
		numGrayProcessed = (long) (counters.findCounter(count.PROCESSED_GRAY)).getValue();
		numGrayUnProcessed = (long) (counters.findCounter(count.UNPROCESSED_GRAY)).getValue();
	}
	public String getInputPath(HadoopJob hJob)
	{
		String inputPath = "";
		if (iterationCount == 0)
			inputPath = "input/"+hJob.getJobName();
		else
			inputPath = "output/"+hJob.getJobName()+"-" + iterationCount;
		return inputPath;
	}
	public String getOutputtPath(HadoopJob hJob)
	{
		String outputPath = "";
		outputPath = "output/"+hJob.getJobName()+"-" + (iterationCount + 1);
		return outputPath;
	}
}
