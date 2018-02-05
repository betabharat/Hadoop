package com.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text kez, Iterable<IntWritable> valColl,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int sum = 0;
		
		for (IntWritable intWritable : valColl) {
			sum += intWritable.get();
		}
		
		context.write(kez, new IntWritable(sum));
	}
	

}
