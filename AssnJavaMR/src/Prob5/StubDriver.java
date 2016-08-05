package Prob5;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StubDriver {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		if (args.length != 2) {
			System.out.printf("Insufficient arguments. Usage: StubDriver <input> <output> \n");
			System.exit(-1);
		}
		
		JobConf jobConf = new JobConf();
		jobConf.set("textinputformat.record.delimiter", ".");
		//Job job = new Job(jobConf, "Recommend");
		Job job = Job.getInstance(jobConf);
		job.setJarByClass(StubDriver.class);
		job.setNumReduceTasks(1);
		job.setGroupingComparatorClass(GroupingComparator.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		job.setMapperClass(StubMapper.class);
		job.setReducerClass(StubReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(TwoWords.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,  new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		System.exit(result?0:1);
	}

}
