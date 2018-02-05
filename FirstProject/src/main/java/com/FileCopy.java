package com; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileCopy {

	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		
		Path source = new Path("/mnt/home/betabharat/Desktop/bharat/a.a");
		Path dest = new Path("/user/betabharat/a.a2");
		
		fs.copyFromLocalFile(source, dest);
		
		
		
		
		
	}
}
