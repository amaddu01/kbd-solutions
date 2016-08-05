package Prob2;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StubMapper extends Mapper<Object,Text,Text,Text>  {
	@Override
	public void map(Object key, Text value, Context context) throws InterruptedException,IOException {
		String line = value.toString().toLowerCase().replaceAll("[^\\da-zA-Z\\s]", "");
		String[] words = line.split("\\s+");
		
		for (String s : words)
		{
			char[] chars= s.toCharArray();
			Arrays.sort(chars);
			String sortedWord = new String(chars);
			context.write(new Text(sortedWord), new Text(s));
		}
	}
}
