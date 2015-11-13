package org.myorg.lr;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.flink.api.java.tuple.Tuple2;


public class TollSegmentState {
	protected static final int historySize = 6; // last 5 minutes + current one

	private ArrayList<MinuteStatistics> lastMinutes; // last 5 speed values
	private NovLav lastNovlav;
	private double segmentToll;
	private double totalAverageSpeed;
	
	private Tuple2<Long, Long> accidentInfo;
	
	public TollSegmentState() {
		lastMinutes = new ArrayList<MinuteStatistics>();
		lastNovlav = new NovLav();
		setSegmentToll(0);
		setTotalAverage(0);
		accidentInfo = new Tuple2<Long,Long>((long) -1,(long) -1);
	}
	
	public static Long getMinute(Long time) {
		return (long) (Math.ceil(time/60000) + 1);
	}
	
	public void setCleared(long time) {
		accidentInfo = new Tuple2(accidentInfo.f0, time);
	}
	
	public void setNewAcc(long time) {
		accidentInfo = new Tuple2(time, accidentInfo.f1);
	}
	
	public void markAndClearAccidents(Tuple2<Boolean, Long> value) {
		synchronized(accidentInfo) {
			if(value.f0 && value.f1 > accidentInfo.f0) {
				// time at which the accident is started
				this.accidentInfo.f0 = value.f1;
				this.accidentInfo.f1 = Long.MAX_VALUE;
			} else if (accidentInfo.f0 > 0 && !value.f0 && 
					TollSegmentState.getMinute(value.f1) > TollSegmentState.getMinute(accidentInfo.f0)) {
				this.accidentInfo.f1 = value.f1;
			}
		}
	}
	
	public boolean needToOutputAccident(long time, int lane) {
		boolean res = false;
		synchronized(accidentInfo) {
			if(lane != 4) {
				// notify vehicles no earlier than the minute following 
				// the minute when the accident occurred
				long minute_vid = TollSegmentState.getMinute(time);
				long minute_acc = TollSegmentState.getMinute(accidentInfo.f0);
				long minute_clear = TollSegmentState.getMinute(accidentInfo.f1);
				if (minute_vid > minute_acc && minute_vid < minute_clear) {
					res = true;
				}
			}
		}
		return res;
	}
	
	public double getCurrentToll(String segid, boolean hasAccident) {
		double vtoll = this.getSegmentToll();
		if(hasAccident && vtoll >0)
				vtoll = 0.0; // toll is 0 for accident segments
		return vtoll;
	}
	
	public void computeTolls(long time, int vid, int segment, int lane, int position, int speed) {
		long minute = getMinute(time);
		this.updateNovLav(minute, vid, speed);
		// compute tolls?
		if(minute > lastNovlav.getMinute()) {
			// compute stats for last minute
			double total_avg = 0.0;
			if(lastMinutes.size() == historySize) {
				total_avg = getTotalAverage()/(historySize-1);
			}
			lastNovlav.setLav(total_avg);
			if(lastMinutes.size() >= 2) {
				// last minute is the current one...
				lastNovlav.setNov(lastMinutes.
					get(lastMinutes.size()-2).vehicleCount());
			}
			lastNovlav.setMinute(minute);
			//System.out.println(last_novlav.get(segid).toString());
			if( (total_avg >= 40 && lastMinutes.size() == historySize) || 
					lastNovlav.getNov() <=50 ) {
				this.setSegmentToll(0);
			} else {
				this.setSegmentToll(2*(lastNovlav.getNov()-50)*(lastNovlav.getNov()-50));
			}
		}
		
	}
	
	private void updateNovLav(long minute, int vid, int speed) {
		if(lastMinutes.size() == 0) {
			MinuteStatistics newminute = new MinuteStatistics();
			newminute.setTime(minute);
			newminute.addVehicleSpeed(vid, speed);
			lastMinutes.add(newminute);
		} else {
			MinuteStatistics lastmin = lastMinutes.get(lastMinutes.size()-1);
			if(lastmin.getTime() == minute) {
				lastmin.addVehicleSpeed(vid, speed);
			} else { // I create a new minute
				// we add to the average the value of the last minute because we won't add to it anymore...
				double tmp_avg = getTotalAverage();
				tmp_avg += lastmin.speedAverage();
				//System.out.println("Average is " + total_average+" min is "+minute);
				if(lastMinutes.size() == historySize) {
					tmp_avg -= lastMinutes.get(0).speedAverage();
					lastMinutes.remove(0);
				}
				setTotalAverage(tmp_avg);
				MinuteStatistics newlastmin = new MinuteStatistics();
				newlastmin.setTime(minute);
				newlastmin.addVehicleSpeed(vid, speed);
				lastMinutes.add(newlastmin);
			}
		}
	}
	
	public double getLav() {
		if(this.lastMinutes.size() < 2)
			return 0.0;
		return this.totalAverageSpeed/(this.lastMinutes.size()-1);
	}
	
	public int getNov() {
		return this.lastNovlav.getNov();
	}

	public double getSegmentToll() {
		return segmentToll;
	}

	public void setSegmentToll(double segmentToll) {
		this.segmentToll = segmentToll;
	}

	public double getTotalAverage() {
		return totalAverageSpeed;
	}

	public void setTotalAverage(double totalAverage) {
		this.totalAverageSpeed = totalAverage;
	}

}
