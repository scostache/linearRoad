package org.myorg.lr;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class OutputAccidentAndToll implements Serializable {
	//private HashMap<Integer, String> previous_segments;
	private HashMap<String, TollSegmentState> segment_statistics;
	
	public OutputAccidentAndToll() {
		//previous_segments = new HashMap<Integer, String>();
		segment_statistics = new HashMap<String, TollSegmentState>();
	}
	
	public double getAverageSpeed(String segid) {
		return segment_statistics.get(segid).getLav();
	}
	
	public int getLastVehicleCount(String segid) {
		return segment_statistics.get(segid).getNov();
	}
	
	public void computeTolls(String segid, long time, int vid, int segment, int lane, int position, int speed) {
		TollSegmentState cstate = this.segment_statistics.get(segid);
		if(cstate == null) {
			cstate = new TollSegmentState();
			this.segment_statistics.put(segid, cstate);
		}
		cstate.computeTolls(time, vid, segment, lane, position, speed);
	}
	
	public Double getCurrentToll(String segid, boolean hasAccident) {
		double vtoll = segment_statistics.get(segid).getCurrentToll(segid, hasAccident);
		return vtoll;
	}
	
	public boolean needToOutputAccident(String segid, long time, int lane) {
		boolean res = false;
		synchronized(segment_statistics) {
			res = segment_statistics.get(segid).needToOutputAccident(time, lane);
		}
		return res;
	}
	
	
	public void markAndClearAccidents(Tuple3<String, Boolean, Long> value) {
		synchronized(segment_statistics) {
			segment_statistics.get(value.f0).markAndClearAccidents(new Tuple2<Boolean, Long>(value.f1, value.f2));
		}
	}

}
