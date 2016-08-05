package Prob4;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StubDriver {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
				if (args.length != 2) {
					System.out.printf("Insufficient arguments. Usage: StubDriver <input> <output> \n");
					System.exit(-1);
				}
				
				JobConf jobConf = new JobConf();
				Job job = new Job(jobConf, "VoterCount");
				job.setJarByClass(StubDriver.class);
				
				job.setMapperClass(StubMapper.class);
				job.setReducerClass(StubReducer.class);
				
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				
				FileInputFormat.addInputPath(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job,  new Path(args[1]));
				
				boolean result = job.waitForCompletion(true);
				System.exit(result?0:1);
	}

}
