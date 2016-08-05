package Prob5;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.PriorityQueue;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class StubReducer extends Reducer<TwoWords,Text,Text,Text> {
	@Override
	public void reduce(TwoWords key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{		
		PriorityQueue<WCEntity> pq = new PriorityQueue<WCEntity>();
		String prevWord = null;
		String curWord = null;
		int count = 0;
		
		for(Text w:values){
			curWord = w.toString();
			if (prevWord == null) {
				count = 1;
				prevWord = curWord;
				continue;
			}
			if (curWord == prevWord) {
				count++;
				prevWord = curWord;
				continue;
			}
			System.err.println(key.toString() + " :" + prevWord + " :" + count);
			new WCEntity(prevWord, count).addToQueue(pq);			
			count = 1;
			prevWord = curWord;
		}
		System.err.println(key.toString() + " :" + curWord + " :" + count);
		new WCEntity(curWord, count).addToQueue(pq);		
		String output = "";
		for (int j = 1; j<=5;j++) {
			WCEntity entity = pq.poll();
			if (entity == null) {
				break;
			}
			output = entity.getWord() + "," + output;			
		}
		output = output.substring(0, output.lastIndexOf(','));
		context.write(new Text(key.getPK()), new Text(output));
	}
}
