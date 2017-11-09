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



public class TFIDF extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( TFIDF.class);

   public static void main( String[] args) throws  Exception {
	
	int res=-1;
	// Run Term Frequency job first.
	int res1= ToolRunner.run( new TermFrequency(),args);
	// If term frequency job is success, run TFIDF Job.
	if(res1==0)
	{
      		res  = ToolRunner.run( new TFIDF(), args);
	}
      	System.exit(res);

   	}

   public int run( String[] args) throws  Exception {

     	Configuration config = new Configuration();
        // Operation to count number of files passed to input.
	int countfiles=0;
 	FileSystem fs=FileSystem.get(config);
	FileStatus[] status=fs.listStatus(new Path(args[0]));
		for (FileStatus s:status)
		{
			countfiles++;
		}

	 // System.out.println("files ",+countfiles); 
	 // Set variable to use in map and reduce classes.
	 config.setInt("nofiles",countfiles);
	 // Configure and create the job class.
	 Job job  = Job .getInstance(config, " tfidf ");
     	 job.setJarByClass( this .getClass());
	 // Set input path for MapReduce jobs.
     	 FileInputFormat.addInputPaths(job,  args[1]);
	 // Set output path for MapReduce jobs.
      	 FileOutputFormat.setOutputPath(job,  new Path(args[ 2]));
	 // Set Map class name for mappers.
      	 job.setMapperClass( Map.class);
	 // Set Map class name for mappers.
      	 job.setReducerClass( Reduce .class);
	 // Set output format of <Key> in mapper.
     	 job.setMapOutputKeyClass(Text.class);
	 // Set output format of <Value> in mapper.
	 job.setMapOutputValueClass(Text.class);
	 // Set output format of <Key> in mappers and recucers.
	 job.setOutputKeyClass(Text.class);
	 // Set output format of <Value> in mappers and recucers.
	 job.setOutputValueClass(DoubleWritable.class);	
	 
      return job.waitForCompletion( true)  ? 0 : 1;
   }

   // Define Map operations.
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();
      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
	 // Get lines from the file
         String line  = lineText.toString();
            if (!line.isEmpty()) {
			// Split <word#####filename value> to <word> <filename value>
                   	String[] parts = line.split("#####");
			// Store <word>
			String keyword = parts[0]; 
			// Store <filename value>
			String tempword = parts[1];
			// Split <filename value> to <filename> and <value>
			String[] parts2=tempword.split("\\s+");	
			// Store filename
		        String filename = parts2[0]; 
			// Store value
			Double valueCount = Double.parseDouble(parts2[1]);
			// Define Map value output in form of <filename=value>
			Text mapvalue=new Text(new StringBuilder().append(filename).append("=").append(valueCount).toString());
 			// Define Map key output: <word>
		        Text currentWord  = new Text(keyword.toString());
			// Produce <Key,Value> pairs
                        context.write(currentWord,mapvalue);
		}
           }
      }
   
 // Define operations of reducer.
 public static class Reduce extends Reducer<Text , Text ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<Text > values,  Context context)
         throws IOException,  InterruptedException {

         int docscount=0;
	 int filep=0;
	 // get count of files
	 Configuration config = context.getConfiguration();
         filep = config.getInt("nofiles",0);
         // Operation to count number of files the word is present and store <file=value> pairs of a word in Arraylist
	 List<String> s1=new ArrayList<String>();
 
		for(Text s: values)
		{	
			docscount++;
		        s1.add(s.toString());
	
			
		}
		// For each pairs in the arryalist , do
		for(String val:s1)
		{
		//Split <filename=value> to <filename> and <value>
                String[] temp=val.split("=");
		//Store filename
		String filename=temp[0];
		//Store value
		Double wf= Double.parseDouble(temp[1]);
		//Calculate IDF value
		Double idf=Math.log10(1+filep/docscount);
		//Calculate TF-IDF
		Double tfidf=wf*idf;
		Text finalkey=new Text();
		//Define key output in the form: <word#####filename>
		finalkey=new Text(new StringBuilder().append(word).append("#####").append(filename).toString());
		//Produce <Key,Value> Pairs : <word####filename, tfidf>
	        context.write(finalkey,  new DoubleWritable(tfidf));
		}
         }
   }
}

