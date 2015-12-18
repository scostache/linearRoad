package org.myorg.lr;

import java.io.Serializable;
import java.util.HashMap;

public class ComputeStatsOperator implements Serializable {
	private HashMap<String, SegmentStatsState> segmentStats;
	private static HashMap<String, Long> segMinutes;
	
	public ComputeStatsOperator() {
		segmentStats = new HashMap<String, SegmentStatsState>();
		segMinutes = new HashMap<String, Long>();
	}
	
	public void computeStats(String segment, LRTuple tuple) {
		SegmentStatsState s= null;
		if(segmentStats.containsKey(segment)) {
			s = segmentStats.get(segment);
		} else {
			s = new SegmentStatsState();
			segmentStats.put(segment, s);
		}
		s.updateAvgStats(tuple.simulated_time, tuple.vid, tuple.speed);
	}
	
	public long getCurrentMinute(String segment) {
		return segMinutes.get(segment);
	}
	
	public double getCurrentAvgSpeed(String segment) {
		if(!this.segmentStats.containsKey(segment)) {
			System.out.println("[LAV] No DATA for segment "+segment);
			return 0.0;
		}
			
		return this.segmentStats.get(segment).getCurrentAvgSpeed();
	}
	
	public int getCurrentNov(String segment) {
		if(!this.segmentStats.containsKey(segment)) {
			System.out.println("[NOV] No DATA for segment "+segment);
			return 0;
		}
		return this.segmentStats.get(segment).getCurrentNov();
	}
	
	public boolean needToOutput(String segment, LRTuple tuple) {
		if(!segmentStats.containsKey(segment))
			return true;
		long minute = segmentStats.get(segment).getCurrentMinute();
		boolean res = false;
		if(!segMinutes.containsKey(segment)) {
			res = true;
			segMinutes.put(segment, minute);
		} else {
			if(segMinutes.get(segment) < minute) {
				res = true;
				segMinutes.put(segment, minute);
			}
		}
		return res;
	}

}
