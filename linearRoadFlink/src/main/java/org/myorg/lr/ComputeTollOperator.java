package org.myorg.lr;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

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
		if(!segmentTolls.containsKey(segid))
			return 0.0;
		
		return segmentTolls.get(segid).getLav();
	}
	
	public int getLastVehicleCount(String segid) {
		if(!segmentTolls.containsKey(segid))
			return 0;
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
	
	public void createIfNotExist(String segid) {
		TollSegmentState cstate = this.segmentTolls.get(segid);
		if(cstate == null) {
			cstate = new TollSegmentState();
			this.segmentTolls.put(segid, cstate);
		}
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
		if(!segmentTolls.containsKey(segid))
			return false;
		return segmentTolls.get(segid).needToOutputAccident(time);
	}
	
	public Tuple2<Long, Long> getAccidentInfo(String segid) {
		if(!segmentTolls.containsKey(segid)) {
			return new Tuple2<Long, Long>((long) -1, Long.MAX_VALUE);
		}
		return segmentTolls.get(segid).getAccidentInfo();
	}
	
	public void markAndClearAccidents(String segid, Tuple3<Long, Integer, Integer> value) {
		String[] segidSplit = segid.split("_");
		Integer segid0 = Integer.parseInt(segidSplit[1]);
		TollSegmentState cstate = this.segmentTolls.get(segid);
		if(cstate == null) {
			cstate = new TollSegmentState();
			this.segmentTolls.put(segid, cstate);
		}
		segmentTolls.get(segid).markAndClearAccidents(new 
				Tuple2<Boolean, Long>(value.f1 == 1, value.f0));
		// we look for other segments in this instance, downstream
		for(int i=1; i<= LinearRoadFlink.NSEGMENTS_ADVANCE; i++) {
			Integer segidprev = segid0-i;
			if(segidprev <0) 
				return;
			String segprev = segidSplit[0]+"_"+segidprev;
			TollSegmentState cstatenext = this.segmentTolls.get(segprev);
			if(cstatenext == null) {
				cstatenext = new TollSegmentState();
				this.segmentTolls.put(segprev, cstatenext);
			}
			segmentTolls.get(segprev).markAndClearAccidents(new 
					Tuple2<Boolean, Long>(value.f1 == 1, value.f0));
		}
	}
	

}
