package reducer;

import helper.VisibleTupleWritable;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends
		Reducer<Text, VisibleTupleWritable, NullWritable, VisibleTupleWritable> {

	@Override
    public void reduce(Text key, Iterable<VisibleTupleWritable> values, Context context)
    		throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String firstRelationName = conf.get("join.relation.first");
		String secondRelationName = conf.get("join.relation.second");
		ArrayList<VisibleTupleWritable> firstRelationElems = new ArrayList<VisibleTupleWritable>();
		ArrayList<VisibleTupleWritable> secondRelationElems = new ArrayList<VisibleTupleWritable>();
		for (VisibleTupleWritable value: values) {
			assert value.size() == 2;
			String actRelationName = value.get(0).toString();
			assert actRelationName.equals(firstRelationName) || actRelationName.equals(secondRelationName);
			if (actRelationName.equals(firstRelationName)) {
				firstRelationElems.add(new VisibleTupleWritable(value.getValuesCopy()));
			} else {
				secondRelationElems.add(new VisibleTupleWritable(value.getValuesCopy()));
			}
		}
		for (VisibleTupleWritable firstRelElem: firstRelationElems) {
			for (VisibleTupleWritable secondRelElem: secondRelationElems) {
				Writable[] resultElems = {firstRelElem.get(1), key, secondRelElem.get(1)};
				context.write(NullWritable.get(), new VisibleTupleWritable(resultElems));
			}
		}
	}

}
