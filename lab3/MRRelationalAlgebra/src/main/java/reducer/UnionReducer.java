package reducer;

import helper.VisibleTupleWritable;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UnionReducer extends
		Reducer<Text, VisibleTupleWritable, NullWritable, VisibleTupleWritable> {

	@Override
    public void reduce(Text key, Iterable<VisibleTupleWritable> values, Context context)
    		throws IOException, InterruptedException {
		VisibleTupleWritable tuple = null;
		for (VisibleTupleWritable actTuple: values) {
			if (tuple == null) {
				tuple = actTuple;
			} else {
				assert tuple.equals(actTuple);
			}
		}
		assert tuple != null;
		context.write(NullWritable.get(), tuple);
    }

}
