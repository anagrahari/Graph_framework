package hadoopComponents;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import java.io.IOException;

public abstract class hadoopReducer extends Reducer<Object, Object,Object, Object>{

	public void reduce(Object key, Iterable<Object> values,
            Context context
            ) throws IOException, InterruptedException {
		
	}
	
}
