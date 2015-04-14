package com.gszpak;

import java.io.IOException;

import mapper.TrianglesMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import reducer.TrianglesReducer;

public class App
{
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "triangles");
	    job.setJarByClass(App.class);
	    job.setMapperClass(TrianglesMapper.class);
	    job.setReducerClass(TrianglesReducer.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
