package org.myorg.lr;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;

import scala.Tuple2;

public class SegmentStatsState {
	protected static final int historyWindow = 5;
	private HashMap<Long,MinuteStatistics> lastMinutes; // last 5 speed values
	private NovLav lastNovlav;
	private double totalAverage;
	private long lastMinute;

	public SegmentStatsState() {
		lastMinutes = new HashMap<Long,MinuteStatistics>();
		lastNovlav = new NovLav();
		totalAverage = 0;
		lastMinute = 0;
	}

	public static Long getMinute(Long time) {
		return (long) (Math.ceil(time/60) + 1);
	}

	public void updateAvgStats(long time, int vid, int speed) {
		long minute = getMinute(time);
		// compute stats for last minute
		this.updateNovLav(minute, vid, speed);
		lastNovlav.setLav(totalAverage);
		// current minute - 1
		MinuteStatistics m = lastMinutes.get(minute - 1);
		if(m != null)
			lastNovlav.setNov(m.vehicleCount());
		else
			lastNovlav.setNov(0);
		lastNovlav.setMinute(minute);
	}
	
	public long getCurrentMinute() {
		return lastNovlav.getMinute();
	}
	
	public double getCurrentAvgSpeed() {
		return lastNovlav.getLav();
	}
	
	public int getCurrentNov() {
		return lastNovlav.getNov();
	}

	private void updateNovLav(long minute, int vid, int speed) {
		if (lastMinutes.size() == 0) {
			MinuteStatistics newminute = new MinuteStatistics();
			newminute.setTime(minute);
			newminute.addVehicleSpeed(vid, speed);
			lastMinutes.put(minute, newminute);
		} else {
			MinuteStatistics currentMinute = lastMinutes.get(minute);
			if(currentMinute != null) {
				currentMinute.addVehicleSpeed(vid, speed);
			} else {
				// current minute is not contained
				MinuteStatistics newlastmin = new MinuteStatistics();
				newlastmin.setTime(minute);
				newlastmin.addVehicleSpeed(vid, speed);
				lastMinutes.put(minute,newlastmin);
			}
			// purge the minutes < current_minute-5
			long smallestMin = minute-5;
			if(lastMinutes.containsKey(smallestMin)) {
				lastMinutes.remove(smallestMin);
			}
		}
	  if (minute > lastMinute) {
		  lastMinute = minute;
		  if(lastMinutes.size() > 1) {
			  double tmp = 0;
			  int count = 0;
			  for(MinuteStatistics m : lastMinutes.values()) {
				  if(m.getTime() == minute)
					  continue;
				  count++;
				  tmp += m.speedAverage();
			  }
			  totalAverage = tmp/count;
		  }
	  }
	}
	
	private class MinuteComparator implements Comparator<MinuteStatistics>
	{
	    @Override
	    public int compare(MinuteStatistics x, MinuteStatistics y)
	    {
	    	// natural order
	        if (x.getTime() < y.getTime()) {
	            return -1;
	        }
	        if (x.getTime() > y.getTime()) {
	            return 1;
	        }
	        return 0;
	    }
	}
}
