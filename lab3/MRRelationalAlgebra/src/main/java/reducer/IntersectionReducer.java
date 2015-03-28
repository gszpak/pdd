package reducer;

import helper.VisibleTupleWritable;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntersectionReducer extends
		Reducer<Text, VisibleTupleWritable, NullWritable, VisibleTupleWritable> {

	@Override
	public void reduce(Text key, Iterable<VisibleTupleWritable> values, Context context)
		throws IOException, InterruptedException {
		VisibleTupleWritable tuple = null;
		int numOfResults = 0;
		for (VisibleTupleWritable actTuple: values) {
			if (tuple == null) {
				tuple = actTuple;
			} else {
				assert tuple.equals(actTuple);
			}
			numOfResults++;
		}
		assert tuple != null;
		if (numOfResults >= 2) {
			context.write(NullWritable.get(), tuple);
		}
	}

}