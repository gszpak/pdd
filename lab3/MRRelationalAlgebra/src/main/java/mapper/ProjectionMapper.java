package mapper;

import helper.CSVLineConverterMixin;
import helper.VisibleTupleWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class ProjectionMapper extends
		Mapper<Object, Text, Text, NullWritable> implements
		CSVLineConverterMixin {

	private Set<Integer> parseColumnNumbers(Context context) {
		Configuration conf = context.getConfiguration();
		String columnNamesString = conf.get("projection.columns");
		String separator = conf.get("projection.separator");
		List<String> columnNames = Arrays.asList(columnNamesString.split(separator));
		return columnNames
				.stream()
				.sequential()
				.map((s) -> Integer.parseInt(s))
				.collect(Collectors.toSet());
	}

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		VisibleTupleWritable tuple = readCSVLineToTuple(value.toString());
		Set<Integer> columnNumbers = parseColumnNumbers(context);
		List<Writable> resultTupleElems = new ArrayList<Writable>();
		for (int i = 0; i < tuple.size(); ++i) {
			if (columnNumbers.contains(i)) {
				resultTupleElems.add(tuple.get(i));
			}
		}
		VisibleTupleWritable resultTuple = new VisibleTupleWritable(resultTupleElems
				.stream()
				.toArray(Writable[]::new));
		context.write(new Text(resultTuple.toString()), NullWritable.get());
	}
}
