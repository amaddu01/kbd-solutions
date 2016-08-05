package Prob1;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FixedLengthInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StubDriver {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 2)
		{
			System.out.printf("Usage: StubDriver <input> <output>\n");
			System.exit(-1);
		}		
		try
		{		
			JobConf jobConf = new JobConf();
			Job job = new Job(jobConf, "LetterFrequency");
			job.setJarByClass(StubDriver.class);
			
			job.setMapperClass(StubMapper.class);
			job.setReducerClass(StubReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(LongWritable.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job,  new Path(args[1]));
			
			boolean result = job.waitForCompletion(true);
			System.exit(result?0:1);
			
		}
		catch(Exception ex)
		{
			throw ex;
		}
	}

}
