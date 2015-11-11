package org.myorg.lr;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class OutputAccidentAndToll {
	private HashMap<String, Tuple2<Long, Long>> accident_segments;
	private HashMap<Integer, String> previous_segments;
	private HashMap<String,ArrayList<MinuteStatistics>> last_minutes; // last 5 speed values
	private HashMap<String,NovLav> last_novlav;
	private HashMap<String, Double> segment_tolls;
	private HashMap<String, Double> total_average;
	
	public OutputAccidentAndToll() {
		accident_segments = new HashMap<String, Tuple2<Long, Long>>();
		previous_segments = new HashMap<Integer, String>();
		last_minutes = new HashMap<String,ArrayList<MinuteStatistics>>();
		last_novlav = new HashMap<String,NovLav>();
		segment_tolls = new HashMap<String, Double>();
		total_average = new HashMap<String, Double>();
	}
	
	public double getAverageSpeed(String segid) {
		return last_novlav.get(segid).getLav();
	}
	
	public int getLastVehicleCount(String segid) {
		return last_novlav.get(segid).getNov();
	}
	
	public void computeTolls(String segid, long time, int vid, int segment, int lane, int position, int speed) {
		long minute = getMinute(time);
		
		if(!last_minutes.containsKey(segid)) {
			last_minutes.put(segid, new ArrayList<MinuteStatistics>());
		}
		if(!last_novlav.containsKey(segid)) {
			last_novlav.put(segid, new NovLav());
		}
		if(!segment_tolls.containsKey(segid)) {
			segment_tolls.put(segid, 0.0);
			total_average.put(segid, 0.0);
		}
		
		this.updateNovLav(segid, minute, vid, speed);
		// compute tolls?
		double toll = 0;
		if(minute > last_novlav.get(segid).getMinute()) {
			// compute stats for last minute
			double total_avg = 0.0;
			if(last_minutes.get(segid).size() == LinearRoadFlink.HistorySize) {
				total_avg = total_average.get(segid)/ (LinearRoadFlink.HistorySize-1);
			}
			last_novlav.get(segid).setLav(total_avg);
			if(last_minutes.get(segid).size() >= 2) {
				// last minute is the current one...
				last_novlav.get(segid).setNov(last_minutes.get(segid).
					get(last_minutes.get(segid).size()-2).vehicleCount());
			}
			last_novlav.get(segid).setMinute(minute);
			//System.out.println(last_novlav.get(segid).toString());
				
			if( (total_avg >= 40 && last_minutes.get(segid).size() == LinearRoadFlink.HistorySize) || 
					last_novlav.get(segid).getNov() <=50 ) {
				toll = 0;
			} else {
				toll = 2*(last_novlav.get(segid).getNov()-50)*(last_novlav.get(segid).getNov()-50);
			}
			segment_tolls.put(segid, toll);
		}
		
	}
	
	private void updateNovLav(String segid, long minute, int vid, int speed) {
		if(last_minutes.get(segid).size() == 0) {
			MinuteStatistics newminute = new MinuteStatistics();
			newminute.setTime(minute);
			newminute.addVehicleSpeed(vid, speed);
			last_minutes.get(segid).add(newminute);
		} else {
			MinuteStatistics lastmin = last_minutes.get(segid).get(last_minutes.get(segid).size()-1);
			if(lastmin.getTime() == minute) {
				lastmin.addVehicleSpeed(vid, speed);
			} else { // I create a new minute
				// we add to the average the value of the last minute because we won't add to it anymore...
				double tmp_avg = total_average.get(segid);
				tmp_avg += lastmin.speedAverage();
				//System.out.println("Average is " + total_average+" min is "+minute);
				if(last_minutes.get(segid).size() == LinearRoadFlink.HistorySize) {
					tmp_avg -= last_minutes.get(segid).get(0).speedAverage();
					last_minutes.get(segid).remove(0);
				}
				total_average.put(segid, tmp_avg);
				MinuteStatistics newlastmin = new MinuteStatistics();
				newlastmin.setTime(minute);
				newlastmin.addVehicleSpeed(vid, speed);
				last_minutes.get(segid).add(newlastmin);
			}
		}
	}
	
	public Double getCurrentToll(String segid) {
		double vtoll = segment_tolls.get(segid);
		synchronized(accident_segments) {
			if(accident_segments.containsKey(segid) && vtoll >0)
				vtoll = 0.0; // toll is 0 for accident segments
		}
		return vtoll;
	}
	
	public boolean vehicleChangedSegment(String segid, Integer vid) {
		if(previous_segments.containsKey(vid)) {
			if(previous_segments.get(vid) == segid)
				return false;
		}
		previous_segments.put(vid, segid);
		return true;
	}
	
	public boolean needToOutputAccident(String segid, long time, int lane) {
		boolean res = false;
		synchronized(accident_segments) {
			if(accident_segments.containsKey(segid) && lane != 4) {
				Tuple2<Long, Long> timeacc = accident_segments.get(segid);
				// notify vehicles no earlier than the minute following 
				// the minute when the accident occurred
				long minute_vid = getMinute(time);
				long minute_acc = getMinute(timeacc.f0);
				long minute_clear = getMinute(timeacc.f1);
				if (minute_vid > minute_acc && time < minute_clear) {
					res = true;
				}
			}
		}
		return res;
	}
	
	
	public void markAndClearAccidents(Tuple3<String, Boolean, Long> value) {
		synchronized(accident_segments) {
			if(!accident_segments.containsKey(value.f0) && value.f1) {
				// time at which the accident is started
				accident_segments.put(value.f0, new Tuple2<Long, Long>(value.f2, Long.MAX_VALUE));
				System.out.println("Putting new accident segment: "+ value.f0+" "+ value.f2);
			} else if (accident_segments.containsKey(value.f0) && !value.f1 && 
					getMinute(value.f2) > getMinute(accident_segments.get(value.f0).f0)) {
				Tuple2<Long, Long> acc = accident_segments.get(value.f0);
				acc.f1 = value.f2; // time at which the accident is cleared
				System.out.println("Clearing accident segment: "+ value.f0+" "+ value.f2);
			}
		}
	}
	
	private static Long getMinute(Long time) {
		return (long) (Math.ceil(time/60000) + 1);
	}

}
