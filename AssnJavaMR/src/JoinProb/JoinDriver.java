package JoinProb;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class JoinDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		if (args.length != 3) {
			System.out.printf("Insufficient arguments. Usage: StubDriver <input> <output> \n");
			System.exit(-1);
		}
		
		JobConf jobConf = new JobConf();		
		Job job = Job.getInstance(jobConf);
		job.setJarByClass(JoinDriver.class);		
		job.setGroupingComparatorClass(JoinGrouping.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, JoinUserMapper.class);
	    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, JoinTweetMapper.class);
		
		job.setReducerClass(JoinReducer.class);
	    //job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(UserLoginKey.class);
		job.setMapOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
				
		boolean result = job.waitForCompletion(true);
		System.exit(result?0:1);
	}

}
