package JoinProb;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class JoinUserMapper extends Mapper<Object,Text,UserLoginKey,Text> {
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String val = value.toString();
		String userlogin = val.substring(0, val.indexOf(","));
		String keyVal = val.substring(val.indexOf(",")+1);
		
		System.err.println(userlogin);
		System.err.println(keyVal);
		
        
		UserLoginKey recordKey = new UserLoginKey(userlogin, UserLoginKey.USER_RECORD);
				                
		//JoinWritable genericRecord = new JoinWritable(new Text(keyVal));
		context.write(recordKey, new Text(keyVal));
	}
}
