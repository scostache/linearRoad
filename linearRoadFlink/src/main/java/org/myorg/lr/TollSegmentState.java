package org.myorg.lr;

import org.apache.flink.api.java.tuple.Tuple2;

public class TollSegmentState {
	private NovLav lastNovlav;
	private double segmentToll;
	private boolean hasAccident;
	private Tuple2<Long, Long> accidentInfo;

	public TollSegmentState() {
		hasAccident = false;
		lastNovlav = new NovLav();
		setSegmentToll(0);
		setAccidentInfo(new Tuple2<Long, Long>((long) -1, (long) -1));
	}

	@Override
	public String toString() {
		return lastNovlav.toString() + " ## " + segmentToll;
	}

	public static Long getMinute(Long time) {
		return (long) (Math.ceil(time / 60) + 1);
	}

	public void setCleared(long time) {
		setAccidentInfo(new Tuple2(getAccidentInfo().f0, time));
	}

	public void setNewAcc(long time) {
		setAccidentInfo(new Tuple2(time, getAccidentInfo().f1));
	}

	public void markAndClearAccidents(Tuple2<Boolean, Long> value) {
		if (value.f0 && value.f1 > getAccidentInfo().f0) {
			// time at which the accident is started
			this.setAccidentInfo(new Tuple2(value.f1, Long.MAX_VALUE));
		} else if (getAccidentInfo().f0 > 0 && !value.f0
				&& TollSegmentState.getMinute(value.f1) > TollSegmentState.getMinute(getAccidentInfo().f0)) {
			this.setAccidentInfo(new Tuple2(getAccidentInfo().f0, value.f1));
		}
		//System.out.println(value.toString() + " ### " + this.getAccidentInfo().toString());
	}

	public static boolean needToOutputAccident(long time, long time_acc, long time_clear) {
		boolean res = false;
		if (time - time_acc > 60 && time_clear < Long.MAX_VALUE) {
			// accident is too old, vehicles might have moved to other segments
			System.out.println("Accident is too old: "+time+" "+time_acc+" "+time_clear);
			return false;
		}
		// notify vehicles no earlier than the minute following
		// the minute when the accident occurred
		long minute_vid = TollSegmentState.getMinute(time);
		long minute_acc = TollSegmentState.getMinute(time_acc);
		long minute_clear = TollSegmentState.getMinute(time_clear);
		System.out.println("Checking minutes for accidents: " + minute_vid+ " "+ minute_acc+" "+minute_clear);
		if (minute_vid >= minute_acc && time > time_acc && minute_vid < minute_clear) {
			System.out.println("Vehicle is in range of accident!");
			res = true;
		}
		System.out.println("Output accident "+res);
		return res;
	}
	
	public boolean needToOutputAccident(long time) {
		return needToOutputAccident(time, this.getAccidentInfo().f0, this.getAccidentInfo().f1);
	}

	public double getCurrentToll(String segid, boolean hasAccident) {
		double vtoll = this.getSegmentToll();
		if (hasAccident && vtoll > 0)
			vtoll = 0.0; // toll is 0 for accident segments
		return vtoll;
	}

	public void computeTolls() {
		if (lastNovlav.getLav() >= 40 || lastNovlav.getNov() <= 50) {
			this.setSegmentToll(0);
		} else {
			this.setSegmentToll(2 * (lastNovlav.getNov() - 50) * (lastNovlav.getNov() - 50));
		}
	}

	public void setLavNov(long minute, double avgspeed, int nvehicles) {
		this.lastNovlav.setLav(avgspeed);
		this.lastNovlav.setNov(nvehicles);
		this.lastNovlav.setMinute(minute);
	}

	public long getCurrentMinute() {
		return this.lastNovlav.getMinute();
	}

	public double getLav() {
		return this.lastNovlav.getLav();
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

	public boolean isHasAccident() {
		return hasAccident;
	}

	public void setHasAccident(boolean hasAccident) {
		this.hasAccident = hasAccident;
	}

	public Tuple2<Long, Long> getAccidentInfo() {
		return accidentInfo;
	}

	public void setAccidentInfo(Tuple2<Long, Long> accidentInfo) {
		this.accidentInfo = accidentInfo;
	}

}
