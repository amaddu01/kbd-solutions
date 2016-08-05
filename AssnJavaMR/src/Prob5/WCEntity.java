package Prob5;

import java.util.Collections;
import java.util.PriorityQueue;


public class WCEntity implements Comparable<WCEntity>{
	private String word;
	private int count;
	
	public WCEntity(String word, int count) {
		this.word = word;
		this.count = count;
	}
	
	public int getCount() {
		return count;
	}

	public String getWord() {
		return word;
	}

	@Override
	public int compareTo(WCEntity arg0) {
		// TODO Auto-generated method stub
		return this.count - arg0.count;
	}
	
	public void addToQueue(PriorityQueue<WCEntity> pq) {
		if (pq.size() == 5) {
			if (this.getCount() > pq.peek().getCount()) {
				pq.poll();
				pq.add(this);
			}
		}
		else {
			pq.add(this);
		}
	}
	
	public static void main(String[] args) {
		PriorityQueue<WCEntity> pq = new PriorityQueue<WCEntity>();
		
		new WCEntity("sandeep", 1).addToQueue(pq);
		new WCEntity("art", 20).addToQueue(pq);
		new WCEntity("art1", 5).addToQueue(pq);
		new WCEntity("sravani", 2).addToQueue(pq);
		new WCEntity("ryan0", 22).addToQueue(pq);
		new WCEntity("ryan2", 3).addToQueue(pq);
		new WCEntity("ryan1", 3).addToQueue(pq);
		
		while(!pq.isEmpty())
		{
			WCEntity wc = pq.poll();
			System.out.println(wc.word + ":" + wc.count);
		}
	}
	
}
