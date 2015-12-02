package org.myorg.lrspark;

import java.util.ArrayList;
import java.util.HashMap;

import org.myorg.lrspark.Vehicle;
import scala.Tuple2;

public class AccidentSegmentState {
	// state for one segment with information about accidents...
	private boolean isNewAccident;
	private boolean isCleared;
	private long timeNew;
	private long timeCleared;
	private long currentTime;
	private HashMap<Integer, Vehicle> vehicles;
	private HashMap<Integer, Vehicle> stopped;
	private HashMap<Integer, ArrayList<Vehicle>> accidents; // <position,
															// stopped_vehicles>

	public AccidentSegmentState() {
		currentTime = 0;
		setVehicles(new HashMap<Integer, Vehicle>());
		stopped = new HashMap<Integer, Vehicle>();
		accidents = new HashMap<Integer, ArrayList<Vehicle>>();
	}

	@Override
	public String toString() {
		String res = "accident:" + " is new:" + this.isNewAccident + " is cleared:" + this.isCleared + " timeNew:"
				+ this.timeNew + " timeCleared:" + this.timeCleared;
		for (Vehicle v : stopped.values()) {
			res += " " + v.id;
		}
		res += " all vehicles: " + getVehicles().size();
		return res;
	}

	public Vehicle addVehicle(int vid, long time, int xway, int lane, int segment, int position, int speed) {
		Vehicle v;
		if (getVehicles().get(vid) == null) {
			v = new Vehicle(vid, time, xway, lane, segment, position);
			getVehicles().put(vid, v);
		} else {
			v = getVehicles().get(vid);
			// int pos, int xway, int seg, int lane, int speed, long time
			v.update(position, xway, segment, lane, speed, time);
		}
		return v;

	}

	public Tuple2<Boolean, Boolean> updateSegmentState(int vid, long time, int xway, int lane, int segment,
			int position, int speed) {
		Vehicle v = this.addVehicle(vid, time, xway, lane, segment, position, speed);
		this.isNewAccident = false;
		this.isCleared = false;
		
		if (v.stopped) {
			// check if it is stopped in the same position as the other vehicles
			addStoppedVehicle(v);
		} else {
			addRunningVehicle(v);
		}
		return new Tuple2<Boolean, Boolean>(this.isNewAccident, this.isCleared);
	}

	protected void addRunningVehicle(Vehicle v) {
		if (stopped.size() == 0) {
			return;
		}
		if (!stopped.containsKey(v.id)) {
			return;
		}
		// vehicle was stopped and is not anymore
		stopped.remove(v.id);
		// we have at least one accident
		if (this.accidents.size() != 0) {
			if (this.accidents.containsKey(v.pos)) {
				ArrayList<Vehicle> vlist = this.accidents.get(v.pos);
				vlist.remove(v);
				// if there are no more vehicles involved in the accident...
				if (vlist.size() <= 1) {
					this.accidents.remove(v.pos);
					if (this.accidents.size() == 0) {
						// if there are no more accidents in the segment
						this.isCleared = true;
						this.timeCleared = v.time;
						this.timeNew = -1;
					}
				}
			}
		}

	}

	protected void addStoppedVehicle(Vehicle v) {
		System.out.println("Adding stopped vehicle " + v.id);
		if (this.stopped.size() == 0) {
			this.stopped.put(v.id, v);
			return;
		}
		if (this.stopped.containsKey(v.id)) {
			if(this.accidents.containsKey(v.pos))
				this.isNewAccident = true;
			return;
		}
		// check if there is an accident
		for (Vehicle elem : this.stopped.values()) {
			if (v.pos == elem.pos && v.lane == elem.lane && (v.time - elem.time) <= 120000 && v.xway == elem.xway) {
				// if accident position is not in array of accidents
				this.isNewAccident = true;
				if (this.accidents.size() == 0) {
					this.timeNew = v.time;
				}
				if (this.accidents.containsKey(v.pos)) {
					// add this vehicle as involved in accident?
					this.accidents.get(v.pos).add(v);
				} else {
					// add another accident to this segment
					ArrayList<Vehicle> vids = new ArrayList<Vehicle>();
					vids.add(v);
					vids.add(elem);
					this.accidents.put(v.pos, vids);
				}
			}
			break;
		}
		stopped.put(v.id, v);
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

	public HashMap<Integer, Vehicle> getVehicles() {
		return vehicles;
	}

	public void setVehicles(HashMap<Integer, Vehicle> vehicles) {
		this.vehicles = vehicles;
	}

}
