package reducer;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GroupReducer extends
		Reducer<Text, Text, NullWritable, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		StringBuilder builder = new StringBuilder(key.toString());
		// Beginning of grouped values
		builder.append(",[");
		for (Text value: values) {
			builder.append(value.toString());
			builder.append(',');
		}
		// Delete last coma
		builder.deleteCharAt(builder.length() - 1);
		// End of grouped values
		builder.append(']');
		context.write(NullWritable.get(), new Text(builder.toString()));
	}

}