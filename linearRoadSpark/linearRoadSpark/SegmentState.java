package linearRoadSpark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.common.base.Optional;

import linearRoadSpark.Vehicle;
import scala.Tuple2;

public class SegmentState implements Serializable{
	  HashMap<Integer,Vehicle> currentVehicles;
	  ArrayList<Integer> stoppedVehicles;
	  int stoppedvehicles;
	  boolean hasNew;
	  boolean isAccident;
	  Long minuteAccident;
	  Long minuteCleared;
	  long cTime;
	  private boolean upstreamAccident;
	  
	  SegmentState() {
		  currentVehicles = new HashMap<Integer,Vehicle>();
		  stoppedVehicles = new ArrayList<Integer>();
		  stoppedvehicles = 0;
		  hasNew = false;
		  minuteAccident = (long) 0;
		  minuteCleared = (long) -1;
		  cTime = 0;
		  upstreamAccident = false;
	  }
	  
	  Vehicle containsVehicle(int vid) {
		  return currentVehicles.get(vid);
	  }
	  
	  void deleteVehicle(int vid) {
		  currentVehicles.remove(vid);
	  }
	  
	  void deleteVehicle(Vehicle v) {
		  if (v.stopped)
			  this.stoppedvehicles--;
		  currentVehicles.remove(v);
	  }
	  
	  void addVehicle(Vehicle v) {
		  currentVehicles.put(v.id, v);
		  this.hasNew = true;
	  }
	  
	  void updateVehicle(Vehicle v, int pos, int lane, int dir, int speed, long time) {
		  v.update(pos, lane, dir, speed, time);
		  if(v.stopped && !v.pastStopped)
			  this.stoppedvehicles++;
		  else if(!v.stopped && v.pastStopped)
			  this.stoppedvehicles--;
			  
	  }
	  
	  boolean hasNewVehicles() {
		  return this.hasNew;
	  }
	  
	  void resetHasNewVehicles() {
		  this.hasNew = false;
	  }
	  
	  boolean hasAccident() {
		  return this.isAccident;
	  }
	  
	  void setAccident() {
		  this.isAccident = true;
		  this.minuteAccident = (long) (System.currentTimeMillis()/60000) + 1;
	  }
	  
	  void resetAccident() {
		  long ctime = System.currentTimeMillis()/1000;
		  if(this.isAccident) {
			  this.isAccident = false;
			  this.minuteCleared = (long) (ctime/60) + 1;
		  } else if ((long) ctime/60 > this.minuteCleared)
			  this.minuteCleared = (long) -1;
	  }

	  @Override
	  public String toString() {
		  String vId = "";
		  for(Integer v : this.currentVehicles.keySet()) {
			  vId += v +",";
		  }
		  return "CTIME: " + this.cTime+ " NV:"+" STOPPED:"+this.stoppedvehicles+" V:"+vId;
	  }

	public boolean isUpstreamAccident() {
		return upstreamAccident;
	}

	public void setUpstreamAccident(boolean upstreamAccident) {
		this.upstreamAccident = upstreamAccident;
	}
}
