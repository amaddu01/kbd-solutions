package Prob5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

@SuppressWarnings("rawtypes")
public class TwoWords implements WritableComparable {
	private String firstWord;
	private String secondWord;
	
	public TwoWords() {		
	}
	
	public TwoWords(String first, String second) {
		this.firstWord = first;
		this.secondWord = second;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		firstWord = WritableUtils.readString(in);
		secondWord = WritableUtils.readString(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, firstWord);
		WritableUtils.writeString(out, secondWord);
	}
	
	@Override
	public String toString() {
		return firstWord+","+secondWord;
	}
	
	public String getPK() {
		return firstWord;
	}
	
	public String getSK() {
		return secondWord;
	}
	
	public void setPK(String f) {
		this.firstWord = f;
	}
	
	public void setSK(String s) {
		this.secondWord = s;
	}
	
	@Override
	public int compareTo(Object o) {
		int result = this.firstWord.compareTo(((TwoWords)o).firstWord );
		if (result == 0){
			result = this.secondWord.compareTo(((TwoWords)o).secondWord);
		}
		return result;
	}
}
