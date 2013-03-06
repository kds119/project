package com.bits.hadoop;
 	
 	import java.io.IOException;
 	import java.util.*;
 	
 	import org.apache.hadoop.fs.Path;
 	
 	import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
 	
 	public class P2PControl {
 	
 	   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
 	     
 		   String keyname = new String();
 		   String keyval = new String();
 		   
 		  @Override
 		    public void configure(JobConf conf)
 		    {
 		        // TODO Auto-generated method stub
 		        super.configure(conf);
 		        
 		        keyname = conf.get("key");
 		        keyval = conf.get("keyval");

 		    }
 		  
 		private final static IntWritable one = new IntWritable(1);
 	    private Text word = new Text();
 	
 	     public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
 	      
 	       
 	       /*StringTokenizer tokenizer = new StringTokenizer(line);
 	       
 	       while (tokenizer.hasMoreTokens()) {
 	       
 	    	 word.set(tokenizer.nextToken());
 	         output.collect(word, one);
 	       }*/
 	    	 String line = value.toString();
 	    	 String[] array = line.split(",");
 	       
 	    	 Mappings mp = new Mappings();
 	       
 	       int index = mp.getIndex(keyname);
 	       
 	       if(array[index].equals(keyval))
 	       {
 	    	   word.set(array[index]);
 	    	   System.out.println("debugging...");
 	       }
 	       else
 	    	   word.set("");
 	       
 	       output.collect(word,one);
 	     }
 	   }
 	
 	   public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
 	     public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
 	       int sum = 0;
 	       
 	       while (values.hasNext()) {
 	         sum += values.next().get();	
 	       }
 	       output.collect(key, new IntWritable(sum));
 	     }
 	   }
 	
 	   public static void main(String[] args) throws Exception {
 	
 		 String key = args[2];  
 		 String keyval = args[3];  
 		 
 		 JobConf conf = new JobConf(P2PControl.class);
 	     conf.setJobName("wordcount");
 	     
 	     conf.set("key", key);
 	     conf.set("keyval", keyval);
 	    
 	     conf.setOutputKeyClass(Text.class);
 	     conf.setOutputValueClass(IntWritable.class);
 	
 	     conf.setMapperClass(Map.class);
 	     //conf.setCombinerClass(Reduce.class);
 	     conf.setReducerClass(Reduce.class);
 	
 	     conf.setInputFormat(TextInputFormat.class);
 	     conf.setOutputFormat(TextOutputFormat.class);
 	
 	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
 	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
 	
 	     JobClient.runJob(conf);
 	   }
 	}