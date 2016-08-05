package JoinProb;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class JoinTweetMapper extends Mapper<Object,Text,UserLoginKey,Text> {
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String val = value.toString();
		String userlogin = val.substring(val.lastIndexOf(",")+1);
		String keyVal = val.substring(0, val.lastIndexOf(","));
		
		System.err.println(userlogin);
		System.err.println(keyVal);
		
        
		UserLoginKey recordKey = new UserLoginKey(userlogin, UserLoginKey.TWITTER_RECORD);
				                
		//JoinWritable genericRecord = new JoinWritable(new Text(keyVal));
		context.write(recordKey, new Text(keyVal));
	}
}
