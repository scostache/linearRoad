package org.myorg.lr;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class DetectAccidentOperator implements Serializable{
	private HashMap<String, AccidentSegmentState> segments;

	public DetectAccidentOperator() {
		segments = new HashMap<String, AccidentSegmentState>();
	}

	public void run(int xway, int segment, long time, int vid, int speed, int lane, int position,
			ArrayList<Tuple3<String, Boolean, Long>> collector) {
		// check if vehicle is stopped or not
		String segid = xway + "_" + segment;
		AccidentSegmentState segstate = segments.get(segid);
		if(segstate == null) {
			segstate = new AccidentSegmentState();
			segments.put(segid, segstate);
		}
		// Type=0, Time, VID, Spd, Xway, Lane, Dir, Seg, Pos
		// int id, long time, int xway, int lane, int seg, int pos
		//segstate.addVehicle(vid, time, xway, lane, segment, position, speed);
		// check for accidents
		Tuple2<Boolean, Boolean> newandcleared = segstate.updateSegmentState(vid, time, xway, lane, segment, position, speed);
		// we output xway_segment, is_accident for all the 4 upstream
		// segments (if is accident)
		// is connected with position stream
		// in the next operator we update an internal state holding the
		// segments_with_accidents
		// and we output accident notification for vehicle
		if ( newandcleared.f0) {
			for (int i = 0; i <= 4; i++) {
				int realseg = segment - i;
				if (realseg < 0)
					break;
				collector.add(new Tuple3<String, Boolean, Long>(xway + "_" + realseg, true, time));
			}
		} else {
			if (newandcleared.f1) {
				collector.add(new Tuple3<String, Boolean, Long>(xway + "_" + segment, false, time));
				for (int i = 1; i <= 4; i++) {
					int realseg = segment - i;
					if (realseg < 0)
						break;
					collector.add(new Tuple3<String, Boolean, Long>(xway + "_" + realseg, false, time));
				}
			}
		}
	}

}
