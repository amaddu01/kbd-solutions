package Prob3a;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StubReducer extends Reducer<Text,Text,Text,Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String res="";
		int count = 0;
		ArrayList<String> list = new ArrayList<String>();
		for (Text text : values) {
			String t = text.toString();
			
			if (!list.contains(t)) {
				count++;
				res+=text.toString()+",";
				list.add(t);
			}
		}
		if (res.endsWith(",")) {
			res = res.substring(0, res.length()-1);
		}
		
		if (count > 1) {
			context.write(key, new Text(res));
		}
	}
}
