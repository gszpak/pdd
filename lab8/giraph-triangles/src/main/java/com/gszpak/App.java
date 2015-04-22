package com.gszpak;

import java.io.IOException;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.IntIntNullTextVertexInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.gszpak.computation.TrianglesComputation;
import com.gszpak.format.MinTextVertexOutputFormat;

public class App {

	public static final IntWritable MIN_VERTEX = new IntWritable(1);

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {
		if (args.length != 2) {
			throw new IllegalArgumentException(
					"run: Must have 2 arguments <input path> <output path>");
		}
		GiraphConfiguration conf = new GiraphConfiguration();
		conf.setVertexInputFormatClass(IntIntNullTextVertexInputFormat.class);
		conf.setVertexOutputFormatClass(MinTextVertexOutputFormat.class);
		conf.setComputationClass(TrianglesComputation.class);
		GiraphJob job = new GiraphJob(conf, "trianglesCounter");
		job.getInternalJob().setJarByClass(App.class);
		FileInputFormat.addInputPath(job.getInternalJob(), new Path(args[0]));
		FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(args[1]));
		System.exit(job.run(true) ? 0 : 1);
	}

}
