package reducer;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DifferenceReducer extends Reducer<Text, Text, NullWritable, Text> {

	private Text remainingRelation = new Text();

	@Override
    public void reduce(Text key, Iterable<Text> values, Context context)
    		throws IOException, InterruptedException {
		String remainingRelationName = context.getConfiguration().get("difference.remaining");
		remainingRelation.set(remainingRelationName);
		boolean shouldRemain = true;
		for (Text relationName: values) {
			if (!relationName.equals(remainingRelation)) {
				shouldRemain = false;
			}
		}
		if (shouldRemain) {
			// Tuple itself serves as a key
			context.write(NullWritable.get(), key);
		}
    }

}
