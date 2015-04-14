package mapper;

import helper.CSVLineConverterMixin;
import helper.UniversalHashFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TrianglesMapper extends Mapper<Object, Text, Text, Text> implements
		CSVLineConverterMixin {

	private void sendEdge(List<Integer> hashTriple, List<Integer> edge,
			Context context) throws IOException, InterruptedException {
		Text mapKey = new Text(tupleToLine(hashTriple));
		Text mapVal = new Text(tupleToLine(edge));
		context.write(mapKey, mapVal);
	}

	private void checkAndSendEdge(int i, int j, int k, int hash1, int hash2,
			List<Integer> edge, Context context) throws IOException, InterruptedException {
		List<Integer> hashTriple = new ArrayList<Integer>(
				Arrays.asList(i, j, k));
		List<Integer> actHashes = new ArrayList<Integer>(Arrays.asList(hash1,
				hash2));
		if (hashTriple.containsAll(actHashes)) {
			sendEdge(hashTriple, edge, context);
		}
	}

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		List<Integer> edge = lineToTuple(value.toString());
		if (edge.size() != 2) {
			throw new IllegalArgumentException();
		}
		int hash1 = UniversalHashFunction.getHash(edge.get(0));
		int hash2 = UniversalHashFunction.getHash(edge.get(1));
		for (int i = 0; i < UniversalHashFunction.P; ++i) {
			for (int j = i + 1; j < UniversalHashFunction.P; ++j) {
				for (int k = j + 1; k < UniversalHashFunction.P; ++k) {
					checkAndSendEdge(i, j, k, hash1, hash2, edge, context);
				}
			}
		}
	}
}
