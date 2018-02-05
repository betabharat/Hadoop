package com.hadoop.wordcount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text arg0, Iterator<IntWritable> arg1, OutputCollector<Text, IntWritable> arg2, Reporter arg3)
			throws IOException {

		int sum = 0;
		while (arg1.hasNext()) {
			sum += arg1.next().get();
		}
		arg2.collect(arg0, new IntWritable(sum));
	}

}
