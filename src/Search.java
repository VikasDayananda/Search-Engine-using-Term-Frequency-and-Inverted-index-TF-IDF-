
/**
Created By
Vikas Dayananda
800969865
vdayanan@uncc.edu
**/

package org.vikas;

import java.lang.Math;         		// Used for Math Operations.
import java.util.ArrayList;     	 // Used for implementing Array lists
import java.util.List;           	  // Used for implementing lists
import java.io.*;  		  	   // Used to work with input and output files
import java.io.File;  		  	   // Used to work with input and output files
import java.io.IOException;        	    // Used for exceptions produced by failed or interrupted I/O operations.
import java.util.regex.Pattern;	    	     // Used for compiling Regular Expressions.
import org.apache.hadoop.fs.FileSystem;       // Used to manage hadoop io files in the program
import org.apache.hadoop.fs.FileStatus;	       // Used to obtain file status of hadoop files in the program.	
import org.apache.hadoop.conf.*;      	        // Used for Configutration of system parameters.
import org.apache.hadoop.conf.Configured;   	 // Used for Configutration of system parameters.  
import org.apache.hadoop.conf.Configuration; 	  // Used for Configutration of system parameters.
import org.apache.hadoop.util.Tool;   		   // A tool interface that supports handling of generic command-line options.
import org.apache.hadoop.util.ToolRunner;     	    // Used to implement Tool interface   
import org.apache.log4j.Logger;   		     // Used for logging services	
import org.apache.hadoop.mapred.JobConf;	      // Used for job configuration services.
import org.apache.hadoop.mapreduce.Job;    	       // Used to handle MapReduce jobs. 
import org.apache.hadoop.mapreduce.Mapper;  		// Used to create map jobs.
import org.apache.hadoop.mapreduce.Reducer; 		 // Used to create reduce jobs.
import org.apache.hadoop.fs.Path;    			  // Used to get hadoop file directories. 
import org.apache.hadoop.io.IntWritable;   	           // Used to Write INT outputs for Mapper and reducers.
import org.apache.hadoop.io.Writable;    		    // Used to Write outputs for Mapper and reducers.
import org.apache.hadoop.io.DoubleWritable;  		     // Used to Write DOUBLE outputs for Mapper and reducers.
import org.apache.hadoop.io.LongWritable;                     // Used to Write LONG outputs for Mapper and reducers.  
import org.apache.hadoop.fs.RemoteIterator; 		       // Used to iterate over hadoop files.      
import org.apache.hadoop.io.Text;                  		// Used to obtain Text format for input and output.
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;    // FileInputFormat is the base class for all file-based InputFormats. 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;   // FileOutputFormat is the base class for all file-based OutputFormats.
import org.apache.hadoop.mapreduce.lib.input.FileSplit;            // Used to obtain filename,length and other attributes from input files.




public class Search extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( Search.class);
   public static void main( String[] args) throws  Exception {
	// Call search job for the input files and write output.	
	int res= ToolRunner.run( new Search(),args);
        System.exit(res);

   }

   public int run( String[] args) throws  Exception {

		Configuration config = new Configuration();
		// Store user query words in array list		
		List<String> queries=new ArrayList<String>();
		for(int i=2;i<args.length;i++)
		{
			queries.add(args[i]);
				
		}

		// Convert array list to array and store it.
		String[] arr = queries.toArray(new String[queries.size()]);
		// Pass array to mapper and reducer class to use.
		config.setStrings("query_array",arr);

	       // Configure and create the job class.
	       Job job  = Job .getInstance(config, " search ");
     	       job.setJarByClass( this .getClass());
	       // Set input path for MapReduce jobs.
     	       FileInputFormat.addInputPaths(job,  args[0]);
	       // Set output path for MapReduce jobs.
      	       FileOutputFormat.setOutputPath(job,  new Path(args[1]));
	       // Set Map class name for mappers.
      	       job.setMapperClass( Map.class);
	       // Set Map class name for mappers.
      	       job.setReducerClass( Reduce .class);
	       // Set output format of <Key> in mapper.
     	       job.setMapOutputKeyClass(Text.class);
	       // Set output format of <Value> in mapper.
	       job.setMapOutputValueClass(DoubleWritable.class);
	       // Set output format of <Key> in mappers and recucers.
	       job.setOutputKeyClass(Text.class);
	       // Set output format of <Value> in mappers and recucers.
	       job.setOutputValueClass(DoubleWritable.class);	
	 
               return job.waitForCompletion( true)  ? 0 : 1;
   }

   // Define operations of Map class   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text , DoubleWritable > {

      private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
	 Configuration config = context.getConfiguration();
        String[] query2 = config.getStrings("query_array");
		
         String line  = lineText.toString();
	//	System.out.println(" QUERYIOASD OJASOIDJKLASJFDKSJFK SDJKJSHFKJSDHFKJHSDFKJHSDKJFHKDSJFHKJSDHFKDSJHFKSJDHFKJSDHFKJ "+USER_QUERY);

            if (!line.isEmpty()) {
                   	
		String[] parts = line.split("#####");
		String keyword = parts[0]; 
		for(String s: query2)
			{
			if(keyword.equalsIgnoreCase(s))
		{
		String tempword = parts[1];
		String[] parts2=tempword.split("\\s+");	

		 String filename = parts2[0]; 
		Double valueCount = Double.parseDouble(parts2[1]);
 
		Text currentWord  = new Text(filename.toString());
		
            context.write(currentWord,new DoubleWritable(valueCount));
		}
}
         }
      
} }  
  // Define operations of Reduce class  
 public static class Reduce extends Reducer<Text , DoubleWritable ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<DoubleWritable > counts,  Context context)
         throws IOException,  InterruptedException {
			//Count total tf-idf value in each files and produce <Key,Value> Pairs.
			double total=0;
			for(DoubleWritable d:counts)
			{
			     total=total+d.get();
			}
		
	                context.write(word,  new DoubleWritable(total));
		}
        }
   }


