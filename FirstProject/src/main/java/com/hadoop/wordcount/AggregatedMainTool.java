package com.hadoop.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils.Text;

public class AggregatedMainTool extends Configured implements Tool{

	public static final String SEPARATOR = "Separator";

	public static void main(String[] args) throws Exception {
		
		System.exit(ToolRunner.run(new AggregatedMainTool(), args));
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		conf.set(SEPARATOR, args[2]);
		
		Job job = new Job(conf, this.getClass().toString());
		job.setJarByClass(getClass());
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setMapperClass(AggregatedMap.class);
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		return job.waitForCompletion(true)?0:1;
		
	}

	
}
