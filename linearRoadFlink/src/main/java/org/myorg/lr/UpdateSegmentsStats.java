package org.myorg.lr;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

public class UpdateSegmentsStats implements FlatMapFunction<LRTuple, Tuple4<String, Long, Integer, Integer>> {

	@Override
	public void flatMap(LRTuple arg0, Collector<Tuple4<String, Long, Integer, Integer>> arg1) throws Exception {
		// TODO Auto-generated method stub

	}

}
