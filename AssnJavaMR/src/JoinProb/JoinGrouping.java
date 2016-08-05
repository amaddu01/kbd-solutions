package JoinProb;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class JoinGrouping extends WritableComparator {
	
	public JoinGrouping() {
        super (UserLoginKey.class, true);
    } 
	
	@SuppressWarnings("rawtypes")
	@Override
    public int compare (WritableComparable a, WritableComparable b){
        UserLoginKey first = (UserLoginKey) a;
        UserLoginKey second = (UserLoginKey) b;
                      
        return first.getPK().compareTo(second.getPK());
    }
}
