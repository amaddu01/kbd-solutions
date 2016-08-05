package Prob4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StubReducer extends Reducer<Text,Text,Text,LongWritable> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String res="";
		ArrayList<String> list = new ArrayList<String>();
		for (Text text : values) {
			list.add(text.toString());			
		}
		String[] a = list.toArray(new String[list.size()]);
		Arrays.sort(a);
		int startIndex = 1;
		int voteVal = 0;
		try {
			voteVal = Integer.parseInt(a[0]);
		}
		catch (NumberFormatException e) {
			startIndex = 0;
		}
		context.write(key, new LongWritable(0));
		for (int j = startIndex;j<a.length;j++) {
			context.write(new Text(a[j]), new LongWritable(voteVal));
		}
		//for (String b:a) {
		//	res+=b+",";
		//}
		//if (res.endsWith(",")) {
		//	res = res.substring(0, res.length()-1);
		//}
		//context.write(key, new Text(res));
		//String[] a = null;
		//a = list.toArray(new String[list.size()]);
		//Arrays.sort(a, Collections.reverseOrder());
		//context.write(key, new LongWritable(0));
		//int voteVal = Integer.parseInt(a[0]);
		//for (int j=1; j < a.length; j++) {
		//	context.write(new Text(a[j]), new LongWritable(voteVal));
		//}
		
	}
}
