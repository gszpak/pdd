package helper;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;

public class VisibleTupleWritable extends TupleWritable {

	public VisibleTupleWritable() {
		super();
	}

	public VisibleTupleWritable(Writable[] values) {
		super(values);
	}

	public Writable[] getValuesCopy() {
		Writable[] valuesCopy = new Writable[this.size()];
		for (int i = 0; i < this.size(); ++i) {
			valuesCopy[i] = this.get(i);
		}
		return valuesCopy;
	}

	public boolean has(int i) {
		return true;
	}

	public String toString() {
		String tupleRepr = super.toString();
		assert tupleRepr.length() >= 2;
		return tupleRepr.substring(1, tupleRepr.length() - 1);
	}
}
