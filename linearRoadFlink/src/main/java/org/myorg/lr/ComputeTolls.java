package org.myorg.lr;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

public class ComputeTolls implements
		CoFlatMapFunction<Tuple3<String, Boolean, Long>, Tuple4<String, Long, Integer, Integer>, Tuple3<String, Integer, Boolean>> {

	@Override
	public void flatMap1(Tuple3<String, Boolean, Long> arg0, Collector<Tuple3<String, Integer, Boolean>> arg1)
			throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void flatMap2(Tuple4<String, Long, Integer, Integer> arg0, Collector<Tuple3<String, Integer, Boolean>> arg1)
			throws Exception {
		// TODO Auto-generated method stub

	}

}
