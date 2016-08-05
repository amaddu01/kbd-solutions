package Prob1;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class StubReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
	{
		long sum = 0;
		for(LongWritable val : values)
		{
			sum += val.get();
		}
		context.write(key, new LongWritable(sum));
	}
}
