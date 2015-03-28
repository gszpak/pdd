package reducer;

import helper.VisibleTupleWritable;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SelectionReducer extends Reducer<Text, VisibleTupleWritable, NullWritable, Text> {

	private VisibleTupleWritable getValue(Iterable<VisibleTupleWritable> values) {
		Iterator<VisibleTupleWritable> it = values.iterator();
		VisibleTupleWritable result = it.next();
		assert !it.hasNext();
		return result;
	}

	private boolean isConditionTrue(Context context, VisibleTupleWritable value) {
		Configuration conf = context.getConfiguration();
		String condString = conf.get("selection.condition");
		String condStringSeparator = conf.get("selection.separator");
		String[] condition = condString.split(condStringSeparator);
		assert condition.length == 3;
		int columnNum = Integer.parseInt(condition[0]);
		String operator = condition[1];
		String toCompare = condition[2];
		int compareResult = value.get(columnNum).toString().compareTo(toCompare);
		if (operator.equals("=")) {
			return compareResult == 0;
		} else if (operator.equals("<>")) {
			return compareResult != 0;
		} else if (operator.equals(">")) {
			return compareResult > 0;
		} else if (operator.equals("<")) {
			return compareResult < 0;
		} else if (operator.equals(">=")) {
			return compareResult >= 0;
		} else if (operator.equals("<=")) {
			return compareResult <= 0;
		} else {
			throw new IllegalArgumentException("Invalid condition in selection");
		}
	}

	@Override
    public void reduce(Text key, Iterable<VisibleTupleWritable> values, Context context)
    		throws IOException, InterruptedException {
		VisibleTupleWritable value = getValue(values);
		if (isConditionTrue(context, value)) {
			// Tuple itself serves as a key
			context.write(NullWritable.get(), key);
		}
    }

}