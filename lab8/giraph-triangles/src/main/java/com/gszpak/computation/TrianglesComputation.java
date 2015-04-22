package com.gszpak.computation;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

public class TrianglesComputation extends
		BasicComputation<IntWritable, IntWritable, NullWritable, IntWritable> {

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

	private void countTriangles(Vertex<IntWritable, IntWritable, NullWritable> vertex,
			Iterable<IntWritable> messages) {
		int result = 0;
		for (IntWritable possibleTriangleVertex: messages) {
			if (vertex.getEdgeValue(possibleTriangleVertex) != null) {
				result++;
			}
		}
		vertex.setValue(new IntWritable(result));
	}

	@Override
	public void compute(Vertex<IntWritable, IntWritable, NullWritable> vertex,
			Iterable<IntWritable> messages) throws IOException {
		if (getSuperstep() == 0) {
			vertex.setValue(new IntWritable(0));
			propagateId(vertex);
		} else if (getSuperstep() == 1) {
			propagatePrevious(vertex, messages);
		} else if (getSuperstep() == 2) {
			countTriangles(vertex, messages);
		}
		vertex.voteToHalt();
	}

}
