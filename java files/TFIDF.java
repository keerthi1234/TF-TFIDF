package kbukkapu;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import java.util.HashMap;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import java.io.FileNotFoundException;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TFIDF extends Configured implements Tool {
private static final Logger LOG = Logger.getLogger( TFIDF.class);
private static final String outputpath = "temp";
 public static void main( String[] args) throws  Exception {
		 //main method invokes and run  the toolrunner 
	      int res  = ToolRunner.run( new TFIDF(), args);
	      System .exit(res);
	   }

	   public int run( String[] args) throws  Exception {
	       //run method configures the job
	     // Path inputpath= new Path(outputpath);  
	      Configuration cnfg= getConf();
	      FileSystem Fsys= FileSystem.get(cnfg);
	      final int totnumfs = Fsys.listStatus(new Path(args[0])).length;
	      cnfg.setInt("totnumfs", totnumfs);
	      Job job  = Job .getInstance(getConf(), "J2");
	      job.setJarByClass( this .getClass());
	      FileInputFormat.setInputPaths (job,  args[0]);
	    //setting the input and output path for the application
	      FileOutputFormat.setOutputPath (job, new Path( args[1]));
	    //setting the map and reduce class for the job
	      job.setMapperClass( M1 .class);
	      job.setReducerClass( R1 .class);
	    //text object is used to use key and the value
	      job.setMapOutputKeyClass( Text .class);
	      job.setMapOutputValueClass( Text .class);
	      return job.waitForCompletion(true) ? 0 : 1;
	    //it will be true only after the completion of job

	 }

		public static class M1 extends Mapper< LongWritable,Text ,  Text ,  Text > {
		      private final static IntWritable one  = new IntWritable( 1);
		      private Text word  = new Text();
		      private static final Pattern WORD_BOUNDARY = Pattern.compile("#####|[\\s]+");
		public void map(LongWritable offset,  Text lineText,  Context context)
		        throws  IOException,  InterruptedException {

		         String line  = lineText.toString();
		 
		    
		       
		       String fname="";
		       String kwd="";
		       String finwd=""; 
		       int i=0;
		       //for loop used to split the line obtained
		        for (String word : WORD_BOUNDARY.split(line)) {
		          if(i==0)
		        	  //word
		       		{  kwd = word; }       		
		       		if(i==1)
		       			//file name
		       		{ fname = word; }         		
		       		if(i==2)
		       		{  
		               finwd = fname+"="+word;
		       		i=0;
		       		context.write(new Text(kwd),new Text(finwd));       		 
		       		}		
		       		i++;       
		        }         
		      }
		  }
	
	   public static class R1 extends Reducer<Text, Text , Text, DoubleWritable> {
		    @Override
		    public void reduce(Text word, Iterable<Text> files, Context context)
		        throws IOException, InterruptedException {
		    	double sum 	=0.0;
		        String key	="";
		           long totnumfs = context.getConfiguration().getInt("totnumfs", 0);
		      // hashmap used to store the output obtained 
		           HashMap<String, Double> TermfreqMap = new HashMap<String,Double>();
		            for ( Text file  : files) 
		           {               
		        	  String fVal = file.toString();
		        	  key = word.toString() + "#####" + fVal.substring(0, fVal.indexOf("="));
		        	 // System.out.println("The KEy %%%%%%%%%%%%%%%%"+ key+"               value is=============="+fVal.substring(fVal.indexOf("=")+1,fVal.length())+"======fval====="+fVal);
		        	  double tFreq = Double.parseDouble( fVal.substring(fVal.indexOf("=")+1,fVal.length()));
		        	  
		        	  //double tFreq = Double.parseDouble ( fVal.substring(fVal.indexOf("=")+1, fVal.length());
		        	  
		        	  TermfreqMap.put(key, tFreq);
		        	  sum++;        	 
		           }
		           //logic for inverse document frequency claculation
		          double idf = Math.log10(1+(totnumfs/sum));          
		           for (String j : TermfreqMap.keySet()) { 
		        	   // calculation of tfidf which will be obtained by multiplication of idf and the term frequency
					 double tfidf = TermfreqMap.get(j)*idf;
						context.write(new Text(j), new DoubleWritable(tfidf));
					}     
		    }
		  }
	   
}



