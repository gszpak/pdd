package reducer;

import helper.CSVLineConverterMixin;
import helper.Position;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TrianglesReducer extends Reducer<Text, Text, NullWritable, Text>
		implements CSVLineConverterMixin {

	private Map<Position, Map<String, Set<String>>> getRelationReprs() {
		Map<Position, Map<String, Set<String>>> relationReprs =
				new HashMap<Position, Map<String, Set<String>>>();
		for (Position p: Position.values()) {
			relationReprs.put(p, new HashMap<String, Set<String>>());
		}
		return relationReprs;
	}

	private void findTriangles(Map<String, Set<String>> left,
			Map<String, Set<String>> middle,
			Map<String, Set<String>> right,
			Context context) throws IOException, InterruptedException {
		for (Map.Entry<String, Set<String>> leftEntry: left.entrySet()) {
			String x = leftEntry.getKey();
			Set<String> yEdges = leftEntry.getValue();
			Set<String> zEdges = middle.get(x);
			if (zEdges == null) {
				continue;
			}
			for (String y: yEdges) {
				for (String z: zEdges) {
					if (right.containsKey(y) && right.get(y).contains(z)) {
						List<String> triangleRepr = new ArrayList<String>(
								Arrays.asList(x, y, z));
						Text output = new Text(tupleToLine(triangleRepr));
						context.write(NullWritable.get(), output);
					}
				}
			}
		}
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Map<Position, Map<String, Set<String>>> relationReprs = getRelationReprs();
		for (Text edgeText: values) {
			List<String> edgeRepr = lineToTuple(edgeText.toString());
			if (edgeRepr.size() != 3) {
				throw new IllegalArgumentException();
			}
			Position p = Position.fromRepr(edgeRepr.get(2));
			Map<String, Set<String>> relation = relationReprs.get(p);
			String node1 = edgeRepr.get(0);
			String node2 = edgeRepr.get(1);
			if (!relation.containsKey(node1)) {
				relation.put(node1, new HashSet<String>());
			}
			relation.get(node1).add(node2);
			relationReprs.put(p, relation);
		}
		findTriangles(relationReprs.get(Position.LEFT), relationReprs.get(Position.MIDDLE),
				relationReprs.get(Position.RIGHT), context);
	}
}
