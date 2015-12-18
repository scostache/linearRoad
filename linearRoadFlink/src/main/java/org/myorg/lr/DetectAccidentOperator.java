package org.myorg.lr;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.myorg.lr.LinearRoadFlink.Partitioner4Segments;

public class DetectAccidentOperator implements Serializable{
	private HashMap<String, AccidentSegmentState> segments;

	public DetectAccidentOperator() {
		segments = new HashMap<String, AccidentSegmentState>();
	}

	public void run(int xway, int segment, long realtime, long time, int vid, int speed, int lane, int position,
			ArrayList<Tuple4<String, Integer, Long, Tuple3<Long,Integer,Integer>>> collector) {
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
		Tuple2<Boolean, Boolean> newandcleared = segstate.
				updateSegmentState(vid, time, xway, lane, segment, position, speed);
		// we output xway_segment, is_accident for all the 4 upstream
		// segments (if is accident)
		// is connected with position stream
		// in the next operator we update an internal state holding the
		// segments_with_accidents
		// and we output accident notification for vehicle
		Partitioner4Segments part = new Partitioner4Segments();
		int cpart = part.partition(new Tuple2<Integer, Integer>(xway, segment), LinearRoadFlink.currentPartitions);
		if (newandcleared.f0) {
			collector.add(new Tuple4<String, Integer, Long, Tuple3<Long,Integer,Integer>>(
					xway + "_" + segment, LinearRoadFlink.ACCIDENT_TYPE, realtime,
					new Tuple3<Long, Integer,Integer>(time,1,1)));
			
			for (int i = 1; i <= LinearRoadFlink.NSEGMENTS_ADVANCE; i++) {
				int realseg = segment - i;	
				if (realseg < 0)
					break;
				int nextpart = part.partition(new Tuple2<Integer,Integer>(xway, realseg), LinearRoadFlink.currentPartitions);
				if(cpart != nextpart)
					collector.add(new Tuple4<String, Integer, Long, Tuple3<Long,Integer,Integer>>(
						xway + "_" + realseg, LinearRoadFlink.ACCIDENT_FW_TYPE, realtime, 
						new Tuple3<Long, Integer,Integer>(time,1,1)));
			}
		} else {
			if (newandcleared.f1) {
				collector.add(new Tuple4<String, Integer, Long, Tuple3<Long,Integer,Integer>>(
						xway + "_" + segment, LinearRoadFlink.ACCIDENT_TYPE, realtime,
						new Tuple3<Long,Integer,Integer>(time, 0, 1)));
				for (int i = 1; i <= LinearRoadFlink.NSEGMENTS_ADVANCE; i++) {
					int realseg = segment - i;
					int nextpart = part.partition(new Tuple2<Integer,Integer>(xway, realseg), LinearRoadFlink.currentPartitions);
					if (realseg < 0)
						break;
					if(cpart != nextpart)
						collector.add(new Tuple4<String, Integer, Long, Tuple3<Long,Integer,Integer>>(
							xway + "_" + realseg, LinearRoadFlink.ACCIDENT_FW_TYPE, realtime,
							new Tuple3<Long,Integer,Integer>(time, 0, 1)));
				}
			} else {
				collector.add(new Tuple4<String, Integer, Long, Tuple3<Long,Integer,Integer>>(
						xway + "_" + segment, LinearRoadFlink.ACCIDENT_TYPE_FAKE, realtime,
						new Tuple3<Long, Integer, Integer>(time, 0, 0)));
				
				for (int i = 1; i <= LinearRoadFlink.NSEGMENTS_ADVANCE; i++) {
					int realseg = segment - i;
					int nextpart = part.partition(new Tuple2<Integer,Integer>(xway, realseg), 
							LinearRoadFlink.currentPartitions);
					if (realseg < 0)
						break;
					if(cpart != nextpart)
						collector.add(new Tuple4<String, Integer, Long, Tuple3<Long,Integer,Integer>>(
							xway + "_" + realseg, LinearRoadFlink.ACCIDENT_FW_FAKE_TYPE, realtime,
							new Tuple3<Long,Integer,Integer>(time, 0, 1)));
				}
			}
		}
	}
}
