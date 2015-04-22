package com.gszpak.computation;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import com.gszpak.App;

public class TrianglesComputation extends
		BasicComputation<IntWritable, IntWritable, NullWritable, IntWritable> {


	private static final IntWritable ZERO_VALUE = new IntWritable(0);


	private void propagateId(Vertex<IntWritable, IntWritable, NullWritable> vertex) {
		for (Edge<IntWritable, NullWritable> outEdge : vertex.getEdges()) {
			if (outEdge.getTargetVertexId().compareTo(vertex.getId()) > 0) {
				sendMessage(outEdge.getTargetVertexId(), vertex.getId());
			}
		}
	}

	private void propagatePrevious(Vertex<IntWritable, IntWritable, NullWritable> vertex,
			Iterable<IntWritable> messages) {
		for (IntWritable previousId: messages) {
			for (Edge<IntWritable, NullWritable> outEdge: vertex.getEdges()) {
				if (outEdge.getTargetVertexId().compareTo(vertex.getId()) > 0) {
					sendMessage(outEdge.getTargetVertexId(), previousId);
				}
			}
		}
	}

	private int countTriangles(Vertex<IntWritable, IntWritable, NullWritable> vertex,
			Iterable<IntWritable> messages) {
		int result = 0;
		for (IntWritable possibleTriangleVertex: messages) {
			if (vertex.getEdgeValue(possibleTriangleVertex) != null) {
				result++;
			}
		}
		return result;
	}

	private int collectResult(Vertex<IntWritable, IntWritable, NullWritable> vertex,
			Iterable<IntWritable> messages) {
		int result = 0;
		for (IntWritable incoming: messages) {
			result += incoming.get();
		}
		return result;
	}

	private void propagateResult(Vertex<IntWritable, IntWritable, NullWritable> vertex,
			Iterable<IntWritable> messages, int result) {
		IntWritable minNeighbour = new IntWritable(Integer.MAX_VALUE);
		for (Edge<IntWritable, NullWritable> outEdge: vertex.getEdges()) {
			if (outEdge.getTargetVertexId().compareTo(minNeighbour) < 0) {
				minNeighbour = outEdge.getTargetVertexId();
			}
		}
		sendMessage(minNeighbour, new IntWritable(result));
	}

	private void saveResult(Vertex<IntWritable, IntWritable, NullWritable> vertex,
			Iterable<IntWritable> messages, int result) {
		assert vertex.getId().equals(App.MIN_VERTEX);
		int actVal = vertex.getValue().equals(ZERO_VALUE) ? 0 : vertex.getValue().get();
		actVal += result;
		vertex.setValue(new IntWritable(actVal));
	}

	private void propagateOrSaveResult(Vertex<IntWritable, IntWritable, NullWritable> vertex,
			Iterable<IntWritable> messages, int result) {
		if (vertex.getId().equals(App.MIN_VERTEX)) {
			saveResult(vertex, messages, result);
		} else {
			propagateResult(vertex, messages, result);
		}
	}

	@Override
	public void compute(Vertex<IntWritable, IntWritable, NullWritable> vertex,
			Iterable<IntWritable> messages) throws IOException {
		if (getSuperstep() == 0) {
			propagateId(vertex);
		} else if (getSuperstep() == 1) {
			propagatePrevious(vertex, messages);
		} else if (getSuperstep() == 2) {
			int result = countTriangles(vertex, messages);
			propagateOrSaveResult(vertex, messages, result);
		} else {
			int result = collectResult(vertex, messages);
			propagateOrSaveResult(vertex, messages, result);
		}
		vertex.voteToHalt();
	}

}
