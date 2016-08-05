package JoinProb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

@SuppressWarnings("rawtypes")
public class UserLoginKey implements WritableComparable {
	
	public static final IntWritable USER_RECORD = new IntWritable(0);
	public static final IntWritable TWITTER_RECORD = new IntWritable(1);
	
	private String userlogin;
	public IntWritable recordType = new IntWritable();

	public UserLoginKey() {		
	}
	
	public UserLoginKey(String userlogin, IntWritable recordType) {
	    this.userlogin = userlogin;
	    this.recordType = recordType;
	}
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		userlogin = WritableUtils.readString(in);
		this.recordType.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		WritableUtils.writeString(out, this.userlogin);
	    this.recordType.write(out);
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		if (this.userlogin.equals(((UserLoginKey)o).userlogin)) {
	        return this.recordType.compareTo(((UserLoginKey)o).recordType);
	    } else {
	        return this.userlogin.compareTo(((UserLoginKey)o).userlogin);
	    }
	}
	
	@Override
	  public int hashCode() {
	    return userlogin.hashCode();
	  }
	
	@Override
	  public boolean equals(Object o) {
		return this.userlogin.equals(((UserLoginKey)o).userlogin) && this.recordType.equals(((UserLoginKey)o).recordType );
	  }
	
		public String getPK() {
			return userlogin;
		}

}
