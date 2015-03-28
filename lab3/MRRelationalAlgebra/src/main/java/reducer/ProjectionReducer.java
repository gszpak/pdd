package reducer;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProjectionReducer extends Reducer<Text, NullWritable, NullWritable, Text> {

	@Override
    public void reduce(Text key, Iterable<NullWritable> values, Context context)
    		throws IOException, InterruptedException {
		// Tuple itself serves as a key
		context.write(NullWritable.get(), key);
    }

}