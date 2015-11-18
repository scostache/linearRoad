package linearRoadSpark;

import scala.Tuple2;

public class TollSegmentState {
	private NovLav lastNovlav;
	private double segmentToll;
	
	private Tuple2<Long, Long> accidentInfo;
	
	public TollSegmentState() {
		lastNovlav = new NovLav();
		setSegmentToll(0);
		accidentInfo = new Tuple2<Long,Long>((long) -1,(long) -1);
	}
	
	@Override
	public String toString() {
		return lastNovlav.toString()+" ## "+segmentToll;
	}
	
	public static Long getMinute(Long time) {
		return (long) (Math.ceil(time/60000) + 1);
	}
	
	public void setCleared(long time) {
		accidentInfo = new Tuple2(accidentInfo._1(), time);
	}
	
	public void setNewAcc(long time) {
		accidentInfo = new Tuple2(time, accidentInfo._2());
	}
	
	public void markAndClearAccidents(Tuple2<Boolean, Long> value) {
			if(value._1() && value._2() > accidentInfo._1()) {
				// time at which the accident is started
				this.accidentInfo = new Tuple2(value._2(), Long.MAX_VALUE);
			} else if (accidentInfo._1 > 0 && !value._1 && 
					TollSegmentState.getMinute(value._2) > TollSegmentState.getMinute(accidentInfo._1)) {
				this.accidentInfo = new Tuple2(accidentInfo._1(), value._2()); //remember time at which is cleared
			}
	}
	
	public boolean needToOutputAccident(long time, int lane) {
		boolean res = false;
		if(time - accidentInfo._1() > 60000 && accidentInfo._2 < Long.MAX_VALUE) {
			// accident is too old, vehicles might have moved to other segments
			return false;
		}
			if( lane != 4) {
				// notify vehicles no earlier than the minute following 
				// the minute when the accident occurred
				long minute_vid = TollSegmentState.getMinute(time);
				long minute_acc = TollSegmentState.getMinute(accidentInfo._1);
				long minute_clear = TollSegmentState.getMinute(accidentInfo._2);
				if (minute_vid > minute_acc && minute_vid < minute_clear) {
					res = true;
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
	
	public void computeTolls() {
			if(lastNovlav.getLav() >= 40 || lastNovlav.getNov() <=50) {
				this.setSegmentToll(0);
			} else {
				this.setSegmentToll(2*(lastNovlav.getNov()-50)*(lastNovlav.getNov()-50));
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

}
