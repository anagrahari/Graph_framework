package hadoopComponents;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public abstract class hadoopMapper extends Mapper<Object, Object, Object, Object>{
	 public void map(Object key, Object input_value, Context context )
				throws IOException, InterruptedException {
	 }
}
