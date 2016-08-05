package Prob4;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StubDriver2 {
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		if (args.length != 2) {
			System.out.printf("Insufficient arguments. Usage: StubDriver <input> <output> \n");
			System.exit(-1);
		}
		
		JobConf jobConf = new JobConf();
		Job job = new Job(jobConf, "VoterCount2");
		job.setJarByClass(StubDriver2.class);
		
		job.setMapperClass(StubMapper2.class);
		job.setReducerClass(StubReducer2.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,  new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		System.exit(result?0:1);
	}
}
