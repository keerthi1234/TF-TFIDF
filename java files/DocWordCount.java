package kbukkapu;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class DocWordCount extends Configured implements Tool {
	

	 private static final Logger LOG = Logger .getLogger( DocWordCount.class);

	   public static void main( String[] args) throws  Exception {
		   //main method invokes and run  the toolrunner 
	      int res  = ToolRunner .run( new DocWordCount(), args);
	      System .exit(res);
	   }

	   public int run( String[] args) throws  Exception {
		   //run method configures the job
	      Job job  = Job .getInstance(getConf(), " wordcount ");
	      job.setJarByClass( this .getClass());

	      FileInputFormat.addInputPaths(job,  args[0]);
	      //setting the input and output path for the application
	      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
	      //setting the map and reduce class for the job
	      job.setMapperClass( Map .class);
	      job.setReducerClass( Reduce .class);
	      //text object is used to use key and the value
	      job.setOutputKeyClass( Text .class);
	      job.setOutputValueClass( IntWritable .class);

	      return job.waitForCompletion( true)  ? 0 : 1;
	      //it will be true only after the completion of job
	   }
	   
	   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
		   //here the map class transforms keyvalue input into the intermediate keyvalue pair
	      private final static IntWritable one  = new IntWritable( 1);
	      private Text word  = new Text();
	      

	      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");
          //creating a regular expression pattern
	      public void map( LongWritable offset,  Text lineText,  Context context)
	        throws  IOException,  InterruptedException {

	         String line  = lineText.toString();
	         Text currentWord  = new Text();
	        //line by line splitting of the lines

	         for ( String word  : WORD_BOUNDARY .split(line)) {
	            if (word.isEmpty()) {
	               continue;
	            }
			String fname = ((FileSplit) context.getInputSplit()).getPath().getName();
			word = word + "#####" + fname;
			//delimiter will be appended between the filename and the word
	            currentWord  = new Text(word);
	            context.write(currentWord,one);
	         }
	      }
	   }

	   public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
	      @Override 
	      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
	         throws IOException,  InterruptedException {
	         int sum  = 0;
	         for ( IntWritable count  : counts) {
	            sum  += count.get();
	         
	         }
	         context.write(word,  new IntWritable(sum));
	      }
	   }
	}
