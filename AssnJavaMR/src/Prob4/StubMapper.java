package Prob4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StubMapper extends Mapper<Object,Text,Text,Text> {
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString().toLowerCase().replaceAll("[^\\da-zA-Z\\s]", "");
		String[] words = line.split("\\s+");
		
		if (words.length == 2) {			
			context.write(new Text(words[0]), new Text(words[1]));						
		}		
	}
}
