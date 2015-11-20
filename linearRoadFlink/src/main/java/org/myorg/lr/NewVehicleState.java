package org.myorg.lr;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;

import scala.Tuple2;

public class NewVehicleState implements Serializable {
	public HashMap<Integer, Long> vehicles;
	public PriorityQueue<Tuple2<Integer, Long>> vehiclesOrdered;
	private long highestTime;
	
	public NewVehicleState() {
		setHighestTime(0);
		vehicles = new HashMap<Integer, Long>();
		vehiclesOrdered = new PriorityQueue<Tuple2<Integer, Long>>(10, new VehicleComparator());
	}
	
	public void addVehicle(int vid, long time) {
		setHighestTime(time);
		vehicles.put(vid, time);
		vehiclesOrdered.add(new Tuple2<Integer, Long> (vid, time));
	}
	
	public boolean containsVehicle(int vid) {
		if(vehicles.containsKey(vid))
			return true;
		return false;
	}
	
	public int getSize() {
		return vehicles.size();
	}
	
	public void removeVehicles(Long timeTh) {
		// start from bottom and remove all vehicles which have time < timeTh
		while(!vehiclesOrdered.isEmpty()) {
			Tuple2<Integer, Long> v = vehiclesOrdered.peek();
			if (v._2() >= timeTh)
				break;
			v = vehiclesOrdered.poll();
			vehicles.remove(v._1);
		}
	}
	
	
	public long getHighestTime() {
		return highestTime;
	}

	public void setHighestTime(long highestTime) {
		this.highestTime = highestTime;
	}


	private class VehicleComparator implements Comparator<Tuple2<Integer, Long>>
	{
	    @Override
	    public int compare(Tuple2<Integer, Long> x, Tuple2<Integer, Long> y) {
	        if (x._2 < y._2) {
	            return -1;
	        }
	        if (x._2 > y._2) {
	            return 1;
	        }
	        return 0;
	    }
	}

}
