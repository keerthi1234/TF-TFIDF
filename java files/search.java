package kbukkapu;
import java.io.Console;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Scanner;

public class search extends Configured implements Tool  {
	private static final Logger LOG = Logger.getLogger(search.class);

    public static void main(String[] args) throws Exception {
    	 //main method invokes and run  the toolrunner 
        int res = ToolRunner.run(new search(), args);
      //run method configures the job
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
    	 String query = args[2];
         getConf().set("query", query);
        Job job = Job.getInstance(getConf(), "wordcount");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
      //setting the input and output path for the application
        //taking tfidf output as the input
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //setting the map and reduce class for the job   
        job.setMapperClass(Map.class);
        
        job.setReducerClass(Reduce.class);
        //text object is used to use key and the value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;
        //it will be true only after the completion of job

    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private final static DoubleWritable one = new DoubleWritable(1);
        private Text word = new Text();
        int c=0;
        
        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

       

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            String line = lineText.toString();
           //query 
            String qry = context.getConfiguration().get("query");
            String[] qp = qry.split("\\s");
           Text currentWord = new Text();
            //line splitting
            String[] wspt = line.split("#####");
            String[] fname = wspt[1].split("\\s");
            //file name to tfidf
            String tfidf = fname[1];
             String wrd = wspt[0];
           for (String s : qp) {
               // comparing the word 
                if (s.equals(wrd)) {
             context.write(new Text(fname[0]), new Text(tfidf));
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text word, Iterable<Text> frequencies, Context context)
                throws IOException, InterruptedException 
        {
            String finx = word.toString();
            Double sum = 0.0;
            for (Text freq : frequencies) 
            {
		     String ct = freq.toString();
             //score calculation
                sum = sum + Double.parseDouble(ct);
            }
			
            String ts = sum.toString();
			 context.write(word, new Text(ts));
        }
    }
}	


