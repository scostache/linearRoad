package org.myorg.lr;

import java.util.Comparator;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class ComparatorAccStats implements Comparator<Tuple> {

	@Override
	public int compare(Tuple o1, Tuple o2) {
		Tuple3<Long, Integer, Integer> t1 = (Tuple3<Long, Integer, Integer>) ((Tuple4<String, Integer, Long, Tuple>) o1).f3;
		Tuple3<Long, Integer, Integer> t2 = (Tuple3<Long, Integer, Integer>) ((Tuple4<String, Integer, Long, Tuple>) o2).f3;
		if(t1.f0 < t2.f0) {
			return -1;
		} else if (t1.f0 > t2.f0)
			return 1;
		return 0;
	}
	
}