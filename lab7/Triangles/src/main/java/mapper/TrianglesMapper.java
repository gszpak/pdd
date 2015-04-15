package mapper;

import helper.CSVLineConverterMixin;
import helper.Position;
import helper.UniversalHashFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TrianglesMapper extends Mapper<Object, Text, Text, Text> implements
		CSVLineConverterMixin {

	class Node implements Comparable<Node> {

		private Integer val;
		private int hash;

		public Node(String nodeRepr) {
			this.val = Integer.valueOf(nodeRepr);
			hash = UniversalHashFunction.getHash(val);
		}

		@Override
		public int compareTo(Node other) {
			if (this.hash < other.hash) {
				return -1;
			} else if (this.hash == other.hash) {
				return this.val.compareTo(other.val);
			} else {
				return 1;
			}
		}

		public String toString() {
			return val.toString();
		}
	}

	private void sendEdge(int hash1, int hash2, int hash3,
			List<Object> mapValRepr, Context context) throws IOException, InterruptedException {
		assert hash1 <= hash2 && hash2 <= hash3;
		List<Integer> hashTriple = new ArrayList<Integer>(
				Arrays.asList(hash1, hash2, hash3));
		Text mapKey = new Text(tupleToLine(hashTriple));
		Text mapVal = new Text(tupleToLine(mapValRepr));
		context.write(mapKey, mapVal);
	}

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		List<String> edgeRepr = lineToTuple(value.toString());
		if (edgeRepr.size() != 2) {
			throw new IllegalArgumentException();
		}
		List<Node> edge = edgeRepr.stream().sequential()
				.map((s) -> new Node(s)).collect(Collectors.toList());
		Collections.sort(edge);
		Node node1 = edge.get(0);
		Node node2 = edge.get(1);
		for (int i = 0; i <= node1.hash; ++i) {
			List<Object> mapValRepr = new ArrayList<Object>(
					Arrays.asList(node1.val, node2.val, Position.RIGHT));
			sendEdge(i, node1.hash, node2.hash, mapValRepr, context);
		}
		for (int i = node1.hash; i <= node2.hash; ++i) {
			List<Object> mapValRepr = new ArrayList<Object>(
					Arrays.asList(node1.val, node2.val, Position.MIDDLE));
			sendEdge(node1.hash, i, node2.hash, mapValRepr, context);
		}
		for (int i = node2.hash; i < UniversalHashFunction.P; ++i) {
			List<Object> mapValRepr = new ArrayList<Object>(
					Arrays.asList(node1.val, node2.val, Position.LEFT));
			sendEdge(node1.hash, node2.hash, i, mapValRepr, context);
		}
	}
}
