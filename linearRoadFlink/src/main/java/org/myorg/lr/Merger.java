package org.myorg.lr;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.flink.api.java.tuple.Tuple4;

public class Merger<Tuple> {
	private ArrayList<String> tupleKeys;
	private HashMap<String,Queue<Tuple>> queue1;
	private HashMap<String,Queue<Tuple>> queue2;
	private Comparator<Tuple> comparator;
	private Tuple maxTuple;
	
	public Merger(Comparator<Tuple> c) {
		setTupleKeys(new ArrayList<String>());
		queue1 = new HashMap<String, Queue<Tuple>>(); //LinkedList<Tuple>();
		queue2 = new HashMap<String, Queue<Tuple>>(); //LinkedList<Tuple>();
		comparator = c;
		maxTuple = null;
	}
	
	public void bufferQueue1(String key, Tuple t) {
		if(!queue1.containsKey(key))
			queue1.put(key, new LinkedList<Tuple>());
		queue1.get(key).add(t);
	}
	
	public void bufferQueue2(String key, Tuple t) {
		if(!queue2.containsKey(key))
			queue2.put(key, new LinkedList<Tuple>());
		queue2.get(key).add(t);
	}
	
	public boolean checkIsReady(String key) {
		if(!this.queue1.containsKey(key) || !this.queue2.containsKey(key))
			return true;
		if(queue1.get(key).isEmpty() && queue2.get(key).isEmpty())
			return true;
		if(!queue1.isEmpty() && !queue2.isEmpty())
			return true;
		return false;
	}
	
	public List<Tuple> getReadyTuples(String key) {
		List<Tuple> result = new LinkedList<Tuple>();
		if(!this.queue1.containsKey(key) || !this.queue2.containsKey(key))
			return result;
		while(!this.queue1.get(key).isEmpty() && !this.queue2.get(key).isEmpty()) {
			Tuple t1 = this.queue1.get(key).peek();
			Tuple t2 = this.queue2.get(key).peek();
			// t1 <= t2: we put t1; queue 1 has priority
			int res = comparator.compare(t1, t2);
			
			if(res == 0) {
				t1 = this.queue1.get(key).poll(); result.add(t1);
				t2 = this.queue2.get(key).poll(); result.add(t2);
				maxTuple = t1;
				//System.out.println("Poll from queue 1 and queue 2 "+queue1.size()+" "+queue2.size());
				//System.out.println(t1.toString()+" "+t2.toString());
			} else if(res < 0) {
				t1 = this.queue1.get(key).poll();
				maxTuple = t1;
				result.add(t1);
			} else {
				result.add(this.queue2.get(key).poll());
			}
		}
		while(!this.queue2.get(key).isEmpty()) {
			if(maxTuple == null)
				break;
			if(comparator.compare(this.queue2.get(key).peek(), maxTuple) >= 0)
				break;
			result.add(this.queue2.get(key).poll());
		}
		return result;
	}

	public ArrayList<String> getTupleKeys() {
		return tupleKeys;
	}

	public void setTupleKeys(ArrayList<String> tupleKeys) {
		this.tupleKeys = tupleKeys;
	}
	
}
