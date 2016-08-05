package Prob5;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StubMapper extends Mapper<Object,Text,TwoWords,Text> {
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString().toLowerCase().replaceAll("[^\\da-zA-Z]", " ");
		String[] words = line.split("\\s+");
		System.err.println(line);
		System.err.println(words.length);
		for (int i = 0; i < words.length-1;i++){
			if (words[i].isEmpty()) {
				continue;
			}
			context.write(new TwoWords(words[i],words[i+1]), new Text(words[i+1]));
		}
	}

}
