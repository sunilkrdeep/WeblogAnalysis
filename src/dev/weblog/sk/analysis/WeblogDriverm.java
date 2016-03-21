package dev.weblog.sk.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.WeblogInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WeblogDriverm extends Configured implements Tool{

@Override
	public int run(String[] args) throws Exception {
	
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		
		job.setJobName(this.getClass().getName());
		
		job.setJarByClass(getClass());

		// configure  input source and output 
		
		WeblogInputFormatm.addInputPath(job, new Path(args[0]));		
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setInputFormatClass(WeblogInputFormatm.class);
		job.setOutputFormatClass(TextOutputFormat.class);		
		
		job.setMapperClass(WeblogMapper.class);
	

		// configure output
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
}
public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WeblogDriverm(), args);
		System.exit(exitCode);
	}
}
