package mapper;

import helper.CSVLineConverterMixin;
import helper.VisibleTupleWritable;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TupleToPairConverterMapper extends
		Mapper<Object, Text, Text, VisibleTupleWritable> implements
		CSVLineConverterMixin {

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		VisibleTupleWritable tuple = readCSVLineToTuple(value.toString());
		context.write(value, tuple);
	}
}
