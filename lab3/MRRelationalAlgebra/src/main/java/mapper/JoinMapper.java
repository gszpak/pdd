package mapper;

import helper.CSVLineConverterMixin;
import helper.VisibleTupleWritable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class JoinMapper extends
		Mapper<Object, Text, Text, VisibleTupleWritable> implements
		CSVLineConverterMixin {

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		VisibleTupleWritable tuple = readCSVLineToTuple(value.toString());
		assert tuple.size() == 2;
		String actRelationName = ((FileSplit) context.getInputSplit()).getPath().getName();
		Configuration conf = context.getConfiguration();
		String firstRelationName = conf.get("join.relation.first");
		String secondRelationName = conf.get("join.relation.second");
		Text resultKey = null;
		VisibleTupleWritable resultPair = null;
		if (actRelationName.equals(firstRelationName)) {
			resultKey = (Text) tuple.get(1);
			Writable[] resultPairElems = {new Text(firstRelationName), tuple.get(0)};
			resultPair = new VisibleTupleWritable(resultPairElems);
		} else if (actRelationName.equals(secondRelationName)) {
			resultKey = (Text) tuple.get(0);
			Writable[] resultPairElems = {new Text(secondRelationName), tuple.get(1)};
			resultPair = new VisibleTupleWritable(resultPairElems);
		} else {
			throw new IllegalArgumentException("Unknown relation name: " + actRelationName);
		}
		context.write(resultKey, resultPair);
	}

}