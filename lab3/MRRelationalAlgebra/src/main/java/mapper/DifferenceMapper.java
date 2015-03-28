package mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class DifferenceMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String actRelationName = ((FileSplit) context.getInputSplit()).getPath().getName();
		context.write(value, new Text(actRelationName));
	}

}
