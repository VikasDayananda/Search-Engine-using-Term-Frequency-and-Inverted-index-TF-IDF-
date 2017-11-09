/**
Created By
Vikas Dayananda
800969865
vdayanan@uncc.edu
**/

package org.vikas;

import java.io.IOException;                         // Used for exceptions produced by failed or interrupted I/O operations.
import java.util.regex.Pattern;                      // Used for compiling Regular Expressions.
import org.apache.hadoop.conf.Configured;             // Used for Configutration of system parameters.
import org.apache.hadoop.util.Tool;		       // A tool interface that supports handling of generic command-line options. 
import org.apache.hadoop.util.ToolRunner;	        // Used to implement Tool interface
import org.apache.log4j.Logger;			         // Used for logging services			
import org.apache.hadoop.mapreduce.Job;		          // Used to handle MapReduce jobs.
import org.apache.hadoop.mapreduce.Mapper;	           // Used to create map jobs.
import org.apache.hadoop.mapreduce.Reducer;	            // Used to create reduce jobs.
import org.apache.hadoop.fs.Path;		             // Used to get hadoop file directories.
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; // FileInputFormat is the base class for all file-based InputFormats. 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;// FileOutputFormat is the base class for all file-based OutputFormats. 
import org.apache.hadoop.io.IntWritable;		        // Used to Write INT outputs for Mapper and reducers.
import org.apache.hadoop.io.LongWritable;	                 // Used to Write LONG outputs for Mapper and reducers.
import org.apache.hadoop.io.Text;			          // Used to obtain Text format for input and output.
import org.apache.hadoop.mapreduce.lib.input.FileSplit;            // Used to obtain filename,length and other attributes from input files.


public class DocWordCount extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( DocWordCount.class);

   public static void main( String[] args) throws  Exception {
      // Call word count job for the input files and write output.
      int res  = ToolRunner .run( new DocWordCount(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      // Configure the job class.
      Job job  = Job .getInstance(getConf(), " wordcount ");
      job.setJarByClass( this .getClass());
      // Set input path for MapReduce jobs.
      FileInputFormat.addInputPaths(job,  args[0]);
      // Set output path for MapReduce jobs.
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      // Set Map class name for mappers.
      job.setMapperClass( Map .class);
      // Set Reduce class name for reducres.
      job.setReducerClass( Reduce .class);
      // Set output format of <Key> in mappers and recucers.
      job.setOutputKeyClass( Text .class);
      // Set output format of <Value> in mappers and recucers.
      job.setOutputValueClass( IntWritable .class);
      // Exit with the status of the job.	
      return job.waitForCompletion( true)  ? 0 : 1;
   }

   // Define Map Job.   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();
      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

	 // Get Lines of the text
         String line  = lineText.toString();
         Text mapKeyOutput  = new Text();
	 // Get the filename
	 String fileName= ((FileSplit)context.getInputSplit()).getPath().getName();
         // For each word, pair a value of 1.
         for ( String word  : WORD_BOUNDARY .split(line)) {
            if (word.isEmpty()) {
               continue;
            }
	    // Build <Key,Value> output: <word#####filename , 1 > 
            mapKeyOutput  = new Text(new StringBuilder().append(word.toLowerCase()).append("#####").append(fileName).toString());
            context.write(mapKeyOutput,one);
         }
      }
   }

   // Define Reduce job.
   public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
      @Override 
      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException {
	 // Count total occurences for all words in the files.
         int sum  = 0;
         for ( IntWritable count  : counts) {
            sum  += count.get();
         }
	 // Output <Key,value> : <word#####filename ,count>
         context.write(word,  new IntWritable(sum));
      }
   }
}
