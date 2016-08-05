package Prob5;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;



public class GroupingComparator extends WritableComparator {
	
	
	protected GroupingComparator() {

		super(TwoWords.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		System.err.println("In comparable...");
		if (w1 == null ) {
			return -1;
		}
		
		if (w2 == null ) {
			return 1;
		}
		TwoWords key1 = (TwoWords) w1;
		TwoWords key2 = (TwoWords) w2;
		
		return key1.getPK().compareTo(key2.getPK());
	}
}
