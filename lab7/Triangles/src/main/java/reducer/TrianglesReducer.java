package reducer;

import helper.CSVLineConverterMixin;
import helper.UniversalHashFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TrianglesReducer extends Reducer<Text, Text, NullWritable, Text>
		implements CSVLineConverterMixin {

	class Graph implements CSVLineConverterMixin {

		class Edge {

			private int node1;
			private int node2;

			public Edge(Integer node1, Integer node2) {
				this.node1 = Math.min(node1.intValue(), node2.intValue());
				this.node2 = Math.max(node1.intValue(), node2.intValue());
			}

			public String toString() {
				return "[" + node1 + ", " + node2 + "]";
			}

			@Override
			public boolean equals(Object o) {
				if (o == this) {
					return true;
				}
				if (!(o instanceof Edge)) {
					return false;
				}
				Edge e = (Edge) o;
				return (this.node1 == e.node1) && (this.node2 == e.node2);
			}

			@Override
			public int hashCode() {
				int result = 17;
				result = 31 * result + this.node1;
				result = 31 * result + this.node2;
				return result;
			}
		}

		private Set<Integer> nodes = new HashSet<Integer>();
		private Set<Edge> edges = new HashSet<Edge>();

		private Graph(Iterable<Text> mapOutput) {
			for (Text edgeText : mapOutput) {
				List<Integer> edgeRepr = lineToTuple(edgeText.toString());
				if (edgeRepr.size() != 2) {
					throw new IllegalArgumentException();
				}
				Integer node1 = edgeRepr.get(0);
				Integer node2 = edgeRepr.get(1);
				nodes.add(node1);
				nodes.add(node2);
				edges.add(new Edge(node1, node2));
			}
		}

		private boolean isEdge(Integer node1, Integer node2) {
			return this.edges.contains(new Edge(node1, node2));
		}

		private List<List<Integer>> getTriangles() {
			List<List<Integer>> triangles = new ArrayList<List<Integer>>();
			Integer[] nodesArr = this.nodes.stream().toArray(Integer[]::new);
			for (int i = 0; i < nodesArr.length; ++i) {
				for (int j = i + 1; j < nodesArr.length; ++j) {
					for (int k = j + 1; k < nodesArr.length; ++k) {
						if (isEdge(nodesArr[i], nodesArr[j])
								&& isEdge(nodesArr[i], nodesArr[k])
								&& isEdge(nodesArr[j], nodesArr[k])) {
							List<Integer> triangle = new ArrayList<Integer>(
									Arrays.asList(nodesArr[i], nodesArr[j],
											nodesArr[k]));
							triangles.add(triangle);
						}
					}
				}
			}
			return triangles;
		}
	}

	private List<Integer> getTriangleHashes(List<Integer> triangle) {
		List<Integer> hashes = triangle
				.stream()
				.sequential()
				.map((i) -> UniversalHashFunction.getHash(i))
				.collect(Collectors.toList());
		Collections.sort(hashes);
		return hashes;
	}

	private boolean isLexicographicallyLargest(List<Integer> hashTriple,
			int actHash) {
		List<Integer> largest = null;
		if (actHash < UniversalHashFunction.P - 2) {
			largest = Arrays.asList(
					actHash,
					UniversalHashFunction.P - 2,
					UniversalHashFunction.P - 1
			);
		} else {
			largest = Arrays.asList(
					UniversalHashFunction.P - 3,
					UniversalHashFunction.P - 2,
					UniversalHashFunction.P - 1
			);
		}
		return hashTriple.equals(largest);
	}

	private boolean isLexicographicallyLargest(List<Integer> hashTriple,
			int actHash1, int actHash2) {
		List<Integer> largestTriple = new ArrayList<Integer>();
		int smaller = Math.min(actHash1, actHash2);
		int larger = Math.max(actHash1, actHash2);
		if (smaller == larger) {
			throw new IllegalArgumentException();
		}

		if (larger >= UniversalHashFunction.P - 3) {
			largestTriple.add(UniversalHashFunction.P - 3);
		} else {
			largestTriple.add(smaller);
		}
		if (larger >= UniversalHashFunction.P - 2) {
			largestTriple.add(UniversalHashFunction.P - 2);
		} else {
			largestTriple.add(larger);
		}
		largestTriple.add(UniversalHashFunction.P - 1);
		return hashTriple.equals(largestTriple);
	}

	private boolean shouldSendToOutput(List<Integer> hashTriple,
			List<Integer> triangle) {
		List<Integer> triangleHashes = getTriangleHashes(triangle);
		int hash1 = triangleHashes.get(0);
		int hash2 = triangleHashes.get(1);
		int hash3 = triangleHashes.get(2);
		// If some hashes are equal, triangle is sent to output
		// only for reducer working with lexicographically largest
		// possible triple of hashes
		if (hash1 == hash2 && hash2 == hash3 && hash1 == hash3) {
			return isLexicographicallyLargest(hashTriple, hash1);
		} else if (hash1 == hash2 || hash2 == hash3 || hash1 == hash3) {
			int h1 = Math.min(hash1, Math.min(hash2, hash3));
			int h2 = Math.max(hash1, Math.max(hash2, hash3));
			return isLexicographicallyLargest(hashTriple, h1, h2);
		} else {
			return true;
		}
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		List<Integer> hashTriple = lineToTuple(key.toString());
		if (hashTriple.size() != 3) {
			throw new IllegalArgumentException();
		}
		Graph inducedGraph = new Graph(values);
		List<List<Integer>> triangles = inducedGraph.getTriangles();
		for (List<Integer> triangle : triangles) {
			if (triangle.size() != 3) {
				throw new IllegalArgumentException();
			}
			if (shouldSendToOutput(hashTriple, triangle)) {
				Text outputTriangle = new Text(tupleToLine(triangle));
				context.write(NullWritable.get(), outputTriangle);
			}
		}
	}
}
