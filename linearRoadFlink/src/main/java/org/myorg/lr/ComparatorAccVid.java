package org.myorg.lr;

import java.util.Comparator;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;

import com.google.common.base.Optional;

public class ComparatorAccVid implements Comparator<Tuple> {

	@Override
	public int compare(Tuple o1, Tuple o2) {
		Tuple7<String, Integer, Long, Long, Long, Long, Optional<LRTuple>> t1 = 
				(Tuple7<String, Integer, Long, Long, Long, Long, Optional<LRTuple>>) o1;
		Tuple7<String, Integer, Long, Long, Long, Long, Optional<LRTuple>> t2 = 
				(Tuple7<String, Integer, Long, Long, Long, Long, Optional<LRTuple>>) o2;
		long time1;
		long time2;
		if(!t1.f6.isPresent()) {
			time1 = t1.f3;
			
		} else {
			LRTuple tup1 = t1.f6.get();
			time1 = tup1.simulated_time;
		}
		if(!t2.f6.isPresent()) {
			time2 = t2.f3;
		} else {
			LRTuple tup2 =t2.f6.get();
			time2 = tup2.simulated_time;
		}
		if(time1 < time2) {
			return -1;
		} else if (time1 > time2)
			return 1;
		return 0;
	}
	
}
