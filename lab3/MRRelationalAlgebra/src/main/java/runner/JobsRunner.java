package runner;

import helper.VisibleTupleWritable;

import java.io.IOException;

import mapper.DifferenceMapper;
import mapper.GroupMapper;
import mapper.JoinMapper;
import mapper.ProjectionMapper;
import mapper.TupleToPairConverterMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import reducer.DifferenceReducer;
import reducer.GroupReducer;
import reducer.IntersectionReducer;
import reducer.JoinReducer;
import reducer.ProjectionReducer;
import reducer.SelectionReducer;
import reducer.UnionReducer;

public class JobsRunner {

	private static final String PROJECTION_COLUMNS_SEPARATOR = "#";
	private static final String SELECTION_CONDITION_SEPARATOR = "#";


	private static void setPaths(Job job, Path[] inputPaths, Path outputPath)
			throws IOException {
		for (Path inputPath: inputPaths) {
			FileInputFormat.addInputPath(job, inputPath);
		}
	    FileOutputFormat.setOutputPath(job, outputPath);
	}

	@SuppressWarnings("rawtypes")
	private static void configureJob(Job job, Class<? extends Mapper> mapperCls,
			Class<? extends Reducer> reducerCls, Class<?> mapOutputKeyCls,
			Class<?> mapOutputValCls, Class<?> outputKeyCls,
			Class<?> outputValCls) throws IOException {
	    job.setJarByClass(JobsRunner.class);
	    job.setMapperClass(mapperCls);
	    job.setReducerClass(reducerCls);
	    job.setMapOutputKeyClass(mapOutputKeyCls);
	    job.setMapOutputValueClass(mapOutputValCls);
	    job.setOutputKeyClass(outputKeyCls);
	    job.setOutputValueClass(outputValCls);
	}

	public static Job runSelection(Configuration conf, Path inputPath,
			Path outputPath, String condition) throws IOException {
	    conf.set("selection.condition", condition);
	    conf.set("selection.separator", SELECTION_CONDITION_SEPARATOR);
		Job job = Job.getInstance(conf, "selection");
	    configureJob(job, TupleToPairConverterMapper.class, SelectionReducer.class,
	    		Text.class, VisibleTupleWritable.class, NullWritable.class, Text.class);
	    Path[] inputPaths = {inputPath};
	    setPaths(job, inputPaths, outputPath);
	    return job;
	}

	public static Job runProjection(Configuration conf, Path inputPath,
			Path outputPath, String columns) throws IOException {
	    conf.set("projection.columns", columns);
	    conf.set("projection.separator", PROJECTION_COLUMNS_SEPARATOR);
		Job job = Job.getInstance(conf, "projection");
	    configureJob(job, ProjectionMapper.class, ProjectionReducer.class,
	    		Text.class, NullWritable.class, NullWritable.class, Text.class);
	    Path[] inputPaths = {inputPath};
	    setPaths(job, inputPaths, outputPath);
	    return job;
	}

	public static Job runUnion(Configuration conf, Path inputPath1,
			Path inputPath2, Path outputPath) throws IOException {
	    Job job = Job.getInstance(conf, "union");
	    configureJob(job, TupleToPairConverterMapper.class, UnionReducer.class,
	    		Text.class, VisibleTupleWritable.class,
	    		NullWritable.class, VisibleTupleWritable.class);
	    Path[] inputPaths = {inputPath1, inputPath2};
	    setPaths(job, inputPaths, outputPath);
	    return job;
	}

	public static Job runIntersection(Configuration conf, Path inputPath1,
			Path inputPath2, Path outputPath) throws IOException {
	    Job job = Job.getInstance(conf, "intersection");
	    configureJob(job, TupleToPairConverterMapper.class, IntersectionReducer.class,
	    		Text.class, VisibleTupleWritable.class,
	    		NullWritable.class, VisibleTupleWritable.class);
	    Path[] inputPaths = {inputPath1, inputPath2};
	    setPaths(job, inputPaths, outputPath);
	    return job;
	}

	public static Job runDifference(Configuration conf, Path inputPath1,
			Path inputPath2, Path outputPath) throws IOException {
		String remaingRelationName = inputPath1.getName();
		conf.set("difference.remaining", remaingRelationName);
		Job job = Job.getInstance(conf, "difference");
	    configureJob(job, DifferenceMapper.class, DifferenceReducer.class,
	    		Text.class, Text.class, NullWritable.class, Text.class);
	    Path[] inputPaths = {inputPath1, inputPath2};
	    setPaths(job, inputPaths, outputPath);
	    return job;
	}

	public static Job runJoin(Configuration conf, Path inputPath1,
			Path inputPath2, Path outputPath) throws IOException {
		conf.set("join.relation.first", inputPath1.getName());
	    conf.set("join.relation.second", inputPath2.getName());
		Job job = Job.getInstance(conf, "join");
	    configureJob(job, JoinMapper.class, JoinReducer.class,
	    		Text.class, VisibleTupleWritable.class,
	    		NullWritable.class, VisibleTupleWritable.class);
	    Path[] inputPaths = {inputPath1, inputPath2};
	    setPaths(job, inputPaths, outputPath);
	    return job;
	}

	public static Job runGroup(Configuration conf, Path inputPath,
			Path outputPath) throws IOException {
		Job job = Job.getInstance(conf, "group");
	    configureJob(job, GroupMapper.class, GroupReducer.class,
	    		Text.class, Text.class, NullWritable.class, Text.class);
	    Path[] inputPaths = {inputPath};
	    setPaths(job, inputPaths, outputPath);
	    return job;
	}

	/**
	 * Usage: java JobsRunner <operation> <operation_args>
	 * Args for operations:
	 * - selection: <input_file> <output_file> <condition>,
	 * 		<condition> = <column_num>#<operator>#<string>,
	 * 		<operator> \in {=, <>, >, <, >=, <=}
	 * - projection: <input_file> <output_file> <column_numbers>,
	 * 		<column_numbers> = <num_1>#<num_2>#...#<num_k>
	 * - union, intersection, difference, join:
	 * 		<input_file_1> <input_file_2> <output_file>
	 * - group: <input_file> <output_file>
	 *
	 */
	public static void main(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException {
	    Configuration conf = new Configuration();
	    String operation = args[0];
	    Job job = null;
	    if (operation.equals("selection")) {
	    	job = runSelection(conf, new Path(args[1]), new Path(args[2]), args[3]);
	    } else if (operation.equals("projection")) {
	    	job = runProjection(conf, new Path(args[1]), new Path(args[2]), args[3]);
	    } else if (operation.equals("union")) {
	    	job = runUnion(conf, new Path(args[1]), new Path(args[2]), new Path(args[3]));
	    } else if (operation.equals("intersection")) {
	    	job = runIntersection(conf, new Path(args[1]), new Path(args[2]), new Path(args[3]));
	    } else if (operation.equals("difference")) {
	    	job = runDifference(conf, new Path(args[1]), new Path(args[2]), new Path(args[3]));
	    } else if (operation.equals("join")) {
	    	job = runJoin(conf, new Path(args[1]), new Path(args[2]), new Path(args[3]));
	    } else if (operation.equals("group")) {
	    	job = runGroup(conf, new Path(args[1]), new Path(args[2]));
	    } else {
	    	throw new IllegalArgumentException("Unknown operation: " + operation);
	    }
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
