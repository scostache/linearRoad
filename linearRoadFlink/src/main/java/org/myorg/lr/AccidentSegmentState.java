package org.myorg.lr;

import java.util.ArrayList;
import java.util.HashMap;

import org.myorg.lr.Vehicle;
import org.apache.flink.api.java.tuple.Tuple2;

public class AccidentSegmentState {
	// state for one segment with information about accidents...
	private boolean hasAccident;
	private boolean isNewAccident;
	private boolean isCleared;
	private long timeNew;
	private long timeCleared;
	private boolean isNewCleared;
	private HashMap<Integer, Vehicle> vehicles;
	private ArrayList<Vehicle> stopped;
	private HashMap<Integer, ArrayList<Vehicle>> accidents; //<position, stopped_vehicles>
	
	public AccidentSegmentState() {
		vehicles = new HashMap<Integer, Vehicle>();
		stopped = new  ArrayList<Vehicle>();
		accidents = new HashMap<Integer, ArrayList<Vehicle>>();
	}
	
	@Override
	public String toString() {
		String res= "accident:"+this.hasAccident+" is new:"+this.isNewAccident+
				" is cleared:"+this.isCleared+" timeNew:"+this.timeNew+" timeCleared:"+this.timeCleared;
		for(Vehicle v: stopped) {
			res+=" "+ v.id;
		}
		res+=" all vehicles: "+vehicles.size();
		return res;
	}
	
	public Vehicle addVehicle(int vid, long time, int xway, int lane, int segment, int position, int speed) {
		Vehicle v;
		if (vehicles.get(vid) == null) {
			v = new Vehicle(vid, time, xway, lane, segment, position);
			vehicles.put(vid, v);
		} else {
			v = vehicles.get(vid);
			// int pos, int xway, int seg, int lane, int speed, long time
			v.update(position, xway, segment, lane, speed, time);
		}
		return v;
		
	}
	
	public Tuple2<Boolean, Boolean> updateSegmentState(int vid, long time, int xway, int lane, int segment, int position, int speed) {
		Vehicle v = this.addVehicle(vid, time, xway, lane, segment, position, speed);
		boolean cleared = false;
		boolean newacc = false;
		if (v.stopped) {
			// check if it is stopped in the same position as the other
			// vehicles
			newacc = addStoppedVehicle(v);
		} else {
			cleared = addRunningVehicle(v);
		}
		return new Tuple2<Boolean, Boolean>(newacc, cleared);
	}
	
	private boolean addRunningVehicle(Vehicle v) {
		boolean cleared = false;
		if (stopped.size() == 0) {
			return cleared;
		}
		if (stopped.contains(v)) {
			// vehicle was stopped and is not anymore
			stopped.remove(v);
			// we have at least one accident
			if (this.accidents.size() !=0 ) {
				if (this.accidents.containsKey(v.pos)) {
					ArrayList<Vehicle> vlist = this.accidents.get(v.pos);
					vlist.remove(v);
					if (vlist.size() <= 1) {
						this.accidents.remove(v.pos);
						if (this.accidents.size() == 0) {
							this.isCleared = true;
							cleared = true;
							this.timeCleared = v.time;
							this.timeNew = -1;
						}
					}
				} 
			}
		}
		return cleared;
	}
	
	private boolean addStoppedVehicle(Vehicle v) {
		System.out.println("Adding stopped vehicle "+v.id);
		if (!this.stopped.contains(v) && this.stopped.size() == 0) {
			this.stopped.add(v);
			return false;
		}
		boolean newacc = false;
			// check if there is an accident
			for (Vehicle elem : this.stopped) {
				if (elem.id == v.id)
					continue;
				if (v.pos == elem.pos && v.lane == elem.lane && (v.time - elem.time) <= 120000
						&& v.xway == elem.xway) {
						// if accident position is not in array of accidents
						if(this.accidents.size() == 0) {
							newacc = true;
							this.isNewAccident = true;
							this.timeNew = v.time;
						}
						if (this.accidents.containsKey(v.pos)) {
							// add this vehicle as involved in accident?
							this.accidents.get(v.pos).add(v);
						} else {
							ArrayList<Vehicle> vids = new ArrayList<Vehicle>();
							vids.add(v);
							vids.add(elem);
							this.accidents.put(v.pos, vids);
						}
					}
					break;
				}
			if (!stopped.contains(v))
				stopped.add(v);
		
		return newacc;
	}

	public boolean hasAccident() {
		return hasAccident;
	}

	public void setAccident(boolean hasAccident) {
		this.hasAccident = hasAccident;
	}

	public boolean isCleared() {
		return isCleared;
	}

	public void setCleared(boolean isCleared) {
		this.isCleared = isCleared;
	}

	public boolean isNewAccident() {
		return isNewAccident;
	}

	public void setNewAccident(boolean isNewAccident) {
		this.isNewAccident = isNewAccident;
	}

	public boolean isNewCleared() {
		return isNewCleared;
	}

	public void setNewCleared(boolean isNewCleared) {
		this.isNewCleared = isNewCleared;
	}

	public long getTimeNew() {
		return timeNew;
	}

	public void setTimeNew(long timeNew) {
		this.timeNew = timeNew;
	}

	public long getTimeCleared() {
		return timeCleared;
	}

	public void setTimeCleared(long timeCleared) {
		this.timeCleared = timeCleared;
	}
	
	
}
