package JoinProb;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class JoinReducer extends Reducer<UserLoginKey,Text,NullWritable,Text> {
	@Override
	public void reduce(UserLoginKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		StringBuilder output = new StringBuilder();
		output.append(key.getPK());
                                             
        for (Text v : values) {
        	output.append(",").append(v.toString());        
        }
        output.append(",").append(key.getPK());
        context.write(NullWritable.get(), new Text(output.toString()));
	}
}
