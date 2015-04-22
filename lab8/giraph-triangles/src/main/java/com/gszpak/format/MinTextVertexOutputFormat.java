package com.gszpak.format;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.gszpak.App;

public class MinTextVertexOutputFormat extends
		TextVertexOutputFormat<IntWritable, IntWritable, Writable> {


	private class MinTextVertexLineWriter extends
			TextVertexWriterToEachLine {

		@Override
		protected Text convertVertexToLine(
				Vertex<IntWritable, IntWritable, Writable> vertex)
				throws IOException {
			System.out.println(vertex.getId().get());
			if (vertex.getId().equals(App.MIN_VERTEX)) {
				return new Text(Integer.toString(vertex.getValue().get()));
			} else {
				return null;
			}
		}
	}


	@Override
	public TextVertexWriter createVertexWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new MinTextVertexLineWriter();
	}
}