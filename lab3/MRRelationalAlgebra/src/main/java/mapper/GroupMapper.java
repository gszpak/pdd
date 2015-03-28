package mapper;

import helper.CSVLineConverterMixin;
import helper.VisibleTupleWritable;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GroupMapper extends
		Mapper<Object, Text, Text, Text> implements
		CSVLineConverterMixin {

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		VisibleTupleWritable tuple = readCSVLineToTuple(value.toString());
		assert tuple.size() == 3;
		Text mapKey = new Text(tuple.get(0).toString());
		Text mapVal = new Text(tuple.get(1).toString());
		context.write(mapKey, mapVal);
	}

}