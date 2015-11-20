package org.myorg.lr;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class ComputeTollOperator implements Serializable {
	//private HashMap<Integer, String> previous_segments;
	private HashMap<String, TollSegmentState> segmentTolls;
	private HashMap<String, Long> lastOutputed;
	
	public ComputeTollOperator() {
		//previous_segments = new HashMap<Integer, String>();
		segmentTolls = new HashMap<String, TollSegmentState>();
		lastOutputed = new HashMap<String, Long>();
	}
	
	public double getAverageSpeed(String segid) {
		return segmentTolls.get(segid).getLav();
	}
	
	public int getLastVehicleCount(String segid) {
		return segmentTolls.get(segid).getNov();
	}
	
	public boolean hasAccident(String seg) {
		return segmentTolls.containsKey(seg) ? segmentTolls.get(seg).isHasAccident() : false;
	}
	
	public boolean needToOutput(String segment, long time) {
		boolean res = false;
		if(!lastOutputed.containsKey(segment)) {
			res = true;
			lastOutputed.put(segment, time);
		} else {
			if(lastOutputed.get(segment) < time) {
				res = true;
				lastOutputed.put(segment, time);
			}
		}
		return res;
	}
	
	
	public void computeTolls(String segid) {
		TollSegmentState cstate = this.segmentTolls.get(segid);
		if(cstate == null) {
			cstate = new TollSegmentState();
			this.segmentTolls.put(segid, cstate);
		}
		cstate.computeTolls();
	}
	
	public void setLavNov(String segid, long minute, int lav, int nov) {
		TollSegmentState cstate = this.segmentTolls.get(segid);
		if(cstate == null) {
			cstate = new TollSegmentState();
			this.segmentTolls.put(segid, cstate);
		}
		segmentTolls.get(segid).setLavNov(minute, lav, nov);
	}
	
	public Double getCurrentToll(String segid, boolean hasAccident) {
		double vtoll = segmentTolls.get(segid).getCurrentToll(segid, hasAccident);
		return vtoll;
	}
	
	public boolean needToOutputAccident(String segid, long time) {
		return segmentTolls.get(segid).needToOutputAccident(time);
	}
	
	public Tuple2<Long, Long> getAccidentInfo(String segid) {
		return segmentTolls.get(segid).getAccidentInfo();
	}
	
	public void markAndClearAccidents(Tuple3<String, Boolean, Long> value) {
		String segid = value.f0;
		TollSegmentState cstate = this.segmentTolls.get(segid);
		if(cstate == null) {
			cstate = new TollSegmentState();
			this.segmentTolls.put(segid, cstate);
		}
		segmentTolls.get(value.f0).markAndClearAccidents(new 
					Tuple2<Boolean, Long>(value.f1, value.f2));
	}

}
