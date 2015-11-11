package org.myorg.lr;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.flink.api.java.tuple.Tuple3;

public class DetectAccidentOperator {
	private HashMap<Integer, Vehicle> vehicles;
	private HashMap<String, ArrayList<Vehicle>> segment_stopped;
	private HashMap<String, ArrayList<Integer>> segments;
	private HashMap<String, HashMap<Integer, ArrayList<Vehicle>>> segment_to_accident;

	public DetectAccidentOperator() {
		vehicles = new HashMap<Integer, Vehicle>();
		segment_stopped = new HashMap<String, ArrayList<Vehicle>>();
		segments = new HashMap<String, ArrayList<Integer>>();
		segment_to_accident = new HashMap<String, HashMap<Integer, ArrayList<Vehicle>>>();
	}

	public void run(int xway, int segment, long time, int vid, int speed, int lane, int position,
			ArrayList<Tuple3<String, Boolean, Long>> collector) {
		// check if vehicle is stopped or not
		String segid = xway + "_" + segment;
		Boolean has_accident = false;

		// Type=0, Time, VID, Spd, Xway, Lane, Dir, Seg, Pos
		// int id, long time, int xway, int lane, int seg, int pos
		Vehicle v = null;
		if (vehicles.get(vid) == null) {
			v = new Vehicle(vid, time, xway, lane, segment, position);
			vehicles.put(vid, v);
		} else {
			v = vehicles.get(vid);
			// int pos, int xway, int seg, int lane, int speed, long time
			v.update(position, xway, segment, lane, speed, time);
		}
		if (!segments.containsKey(segid)) {
			ArrayList elems = new ArrayList<Integer>();
			elems.add(vid);
			segments.put(segid, elems);
		} else {
			if (!segments.get(segid).contains(vid))
				segments.get(segid).add(vid);
		}

		boolean cleared = false;
		boolean newacc = false;

		if (v.stopped) {
			System.out.println("vid: " + v.id + " stopped: " + v.stopped + " segid: " + segid);
			// check if it is stopped in the same position as the other
			// vehicles
			newacc = addStoppedVehicle(v, segid);
		} else {
			cleared = addRunningVehicle(v, segid);
		}

		// we output xway_segment, is_accident for all the 4 upstream
		// segments (if is accident)
		// is connected with position stream
		// in the next operator we update an internal state holding the
		// segments_with_accidents
		// and we output accident notification for vehicle
		if (segment_to_accident.containsKey(segid) && newacc) {
			for (int i = 0; i <= 4; i++) {
				int realseg = segment - i;
				if (realseg < 0)
					break;
				collector.add(new Tuple3<String, Boolean, Long>(xway + "_" + realseg, true, time));
			}
		} else {
			if (!segment_to_accident.containsKey(segid) && cleared) {
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
	
	private boolean addRunningVehicle(Vehicle v, String segid) {
		boolean cleared = false;
		ArrayList<Vehicle> vehicles = segment_stopped.get(segid);
		if (vehicles == null) {
			return cleared;
		}
		if (vehicles.contains(v)) {
			// vehicle was stopped and is not anymore
			vehicles.remove(v);
			if (segment_to_accident.containsKey(segid)) {
				if (segment_to_accident.get(segid).containsKey(v.pos)) {
					ArrayList<Vehicle> vlist = segment_to_accident.get(segid).get(v.pos);
					vlist.remove(v);
					if (vlist.size() <= 1) {
						segment_to_accident.get(segid).remove(v.pos);
						if (segment_to_accident.get(segid).size() == 0) {
							segment_to_accident.remove(segid);
							cleared = true;
						}
					}
				}
			}
		}
		return cleared;
	}
	
	private boolean addStoppedVehicle(Vehicle v, String segid) {
		boolean newacc = false;
		if (!segment_stopped.containsKey(v)) {
			ArrayList<Vehicle> elems = new ArrayList<Vehicle>();
			elems.add(v);
			segment_stopped.put(segid, elems);
		} else {
			ArrayList<Vehicle> vehicles = segment_stopped.get(segid);
			// check if there is an accident
			for (Vehicle elem : vehicles) {
				if (elem.id == v.id)
					continue;
				if (v.pos == elem.pos && v.lane == elem.lane && (v.time - elem.time) <= 120000
						&& v.xway == elem.xway) {
					System.out.println("Segment " + segid + " has accident!");
					if (!segment_to_accident.containsKey(segid)) {
						HashMap<Integer, ArrayList<Vehicle>> accident_map = new HashMap<Integer, ArrayList<Vehicle>>();
						ArrayList<Vehicle> vids = new ArrayList<Vehicle>();
						vids.add(v);
						vids.add(elem);
						accident_map.put(v.pos, vids);
						segment_to_accident.put(segid, accident_map);
						newacc = true;
					} else {
						// if accident position is not in array of accidents
						HashMap<Integer, ArrayList<Vehicle>> accident_map = segment_to_accident.get(segid);
						if (accident_map.containsKey(v.pos)) {
							// add this vehicle as involved in accident?
							accident_map.get(v.pos).add(v);
						} else {
							ArrayList<Vehicle> vids = new ArrayList<Vehicle>();
							vids.add(v);
							vids.add(elem);
							accident_map.put(v.pos, vids);
						}
					}
					break;
				}
			}
			if (!vehicles.contains(v))
				vehicles.add(v);
		}
		return newacc;
	}

}
