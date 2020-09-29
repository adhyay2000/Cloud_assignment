// package com.project.jobUtils;
// import java.io.IOException;
// import java.util.*;
// import com.project.utils.contracts.DBManager;
// import com.project.utils.Table;
// import com.project.utils.AggFunc;
// import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.conf.*;
// import org.apache.hadoop.io.*;
// import org.apache.hadoop.mapreduce.*;
// import org.apache.hadoop.util.*;
// import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
// import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


// public class Groupby{

// 	public static Output execute(Input input) throws IOException,SQLException,InterruptedException, ClassNotFoundException{
// 		Output output = new Output();

//                 Configuration conf = new Configuration();    
//                 // like defined in hdfs-site.xml (required for reading file from hdfs)
//                 conf.set("fs.defaultFS", Globals.getNamenodeUrl());
//                 // defining properties to be used later by mapper and reducer
//                 conf.setEnum("table", input.getTable());
//                 conf.setEnum("aggregateFunction", input.getAggFunc());
//                 conf.setInt("comparisonNumber", input.getComparisonNumber());
//                 conf.setStrings("columns", input.getColumns().toArray());
//                 conf.setInt("aggCol",input.getFnCol());
//                 // conf.setStrings("operationColumns", input.getOperationColumns().toArray(new String[0]));
//                 // creating job and defining jar
//                 Job job = Job.getInstance(conf, "Groupby");
//                 job.setJarByClass(Groupby.class);
//                 job.serMapperClass(GroupbyMapper.class);
//                 // setting combiner class
//                 job.setCombinerClass(GroupbyCombiner.class);
//                 // setting the reducer
//                 job.setReducerClass(GroupbyReducer.class);
//                 job.setOutputKeyClass(Text.class);
//                 job.setOutputValueClass(Text.class);
//                 FileInputFormat.addInputPath(job,new Path(Globals.getCsvInputPath()+DBManager.getFileName(input.table)));
//                 // defining path of output file
//                 Path outputPath = new Path(Globals.getHadoopOutputPath());
//                 FileOutputFormat.setOutputPath(job, outputPath);
//                 // deleting existing outputPath file to allow reusability
//                 outputPath.getFileSystem(conf).delete(outputPath, true);
//                 long startTime = Time.now();
//                 long endTime = (job.waitForCompletion(true) ? Time.now() : startTime);
//                 long execTime = endTime - startTime;
//                 output.setHadoopExecutionTime(execTime);
// 	}
// 	public static class GroupbyMapper extends Mapper<Object,Text,Text,IntWritable>{
// 		private Table table;
//                 private static String[] columns;
//                 private static AggregateFunction aggregateFunction;
//                 private static int comparisonNumber;
//                 private static int aggCol;
//                 @Override
//                 protected void setup(Context context) throws IOException, InterruptedException {
//                     Configuration conf = context.getConfiguration();
//                     columns = conf.getStrings("columns");
//                     aggregateFunction = conf.getEnum("aggregateFunction", AggFunc.NONE);
//                     table = conf.getEnum("table", Tables.NONE);
//                     aggCol = conf.getInt("aggCol");
//                     comparisonNumber = conf.getInt("comparisonNumber", Integer.MIN_VALUE);
//                     super.setup(context);
//                 }
//                 public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
// 			String[] record = value.toString().split(";");
//                         StringBuilder builder = new StringBuilder();
//                         for(int i=0;i<columns.length-1;i++){
//                                 builder.append(record[DBManager.getColumnIndex(table,columns[i])]).append(";");
//                         }
//                         builder.append(record[DBManager.getColumnIndex(table,columns[columns.length-1])]);
//                         Text outKey = new Text(builder.toString());
//                         Int outValue = Integer.parseInt(record[aggCol]);
//                         switch(aggregateFunction){
//                                 case MAX:
//                                 case MIN:
//                                         if(outValue>comparisonNumber){
//                                                 context.write(keyOut,new Text(Integer.toString(outValue)));
//                                         }
//                                         break;
//                                 case SUM:
//                                         context.write(keyOut,new Text(Integer.toString(outValue)));
//                                         break;
//                                 case COUNT:
//                                     context.write(keyOut, new Text("1"));
//                                     break;
//                                 default:
//                                     // not likely to be encountered
//                                     throw new IllegalArgumentException("The aggregate function is not valid");
//                         }
//                 }
// 	}
//         public static class GroupbyReducer extends Reducer<Object,Object,Object,Objecy>{
//                 public void reduce(Object key,Object value,Context context) throws IOException,InterruptedException{
                	
//                 }
//         }
// }


// Instead of column name i will have column number,
// and I will tokenize a row in ArrayList and traverse using column number

// PSEUDO CODE: