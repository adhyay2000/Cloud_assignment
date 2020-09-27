package com.project.jobUtils;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Groupby{
	public static Output execute(Input input) throws IOException,SQLException,InterruptedException, ClassNotFoundException{
		Output output = new Output();

        Configuration conf = new Configuration();
        
        // like defined in hdfs-site.xml (required for reading file from hdfs)
        conf.set("fs.defaultFS", Globals.getNamenodeUrl());

        // defining properties to be used later by mapper and reducer
        conf.setEnum("table", input.getTable());
        conf.setEnum("aggregateFunction", input.getAggFunc());
        conf.setInt("comparisonNumber", input.getComparisonNumber());
        // conf.setStrings("columns", input.getColumns());
        // conf.setStrings("operationColumns", input.getOperationColumns().toArray(new String[0]));

        // creating job and defining jar
        Job job = Job.getInstance(conf, "Groupby");
        job.setJarByClass(Groupby.class);
        // setting combiner class
        job.setCombinerClass(GroupbyCombiner.class);
        // setting the reducer
        job.setReducerClass(GroupbyReducer.class);
        
	}
	public static class GroupbyMapper extends Mapper<Object,Text,Text,IntWritable>{

	}
}


Instead of column name i will have column number,
and I will tokenize a row in ArrayList and traverse using column number

PSEUDO CODE: