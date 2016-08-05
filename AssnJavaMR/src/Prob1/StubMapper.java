package Prob1;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class StubMapper extends Mapper<Object, Text, Text, LongWritable> {
	@Override
	public void map(Object key, Text value, Context context) throws InterruptedException, IOException
	{
		String line = value.toString().toLowerCase().replaceAll("[^\\da-zA-Z]", "");
		
		char[] chars = line.toCharArray();
		for (Character c : chars)
		{
			context.write(new Text(c.toString()), new LongWritable(1));
		}
			
		
	}
}
