package linearRoadSpark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.common.base.Optional;

import linearRoadSpark.Vehicle;
import scala.Tuple2;

public class SegmentStateAlternative implements Serializable{
	  ArrayList<Tuple2<Integer, Integer>> stoppedVehicles;
	  boolean isAccident;
	  Long minuteAccident;
	  Long minuteCleared;
	  int cTime;
	  private boolean upstreamAccident;
	  Integer accidentPosition;
	  
	  SegmentStateAlternative() {
		  stoppedVehicles = new ArrayList<Tuple2<Integer, Integer>>();
		  accidentPosition = -1;
		  minuteAccident = (long) 0;
		  minuteCleared = (long) -1;
		  cTime = 0;
		  upstreamAccident = false;
	  }
	  
	  boolean checkAccident() {
		 boolean isAccident = false;
		 int size = stoppedVehicles.size();
		 for (int i = 0; i < size-1; i++) {
			 for(int j=i+1; j < size; j++) {
				 if (stoppedVehicles.get(i)._2 == stoppedVehicles.get(j)._2) {
					 isAccident = true;
					 break;
				 }
			 }
		 }
		 return isAccident;
	  }
	  
	  boolean isAccident() {
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
	  
	  public Tuple2<Integer, Integer> isInStoppedVehicles(int id) {
		  for(Tuple2<Integer, Integer> t : this.stoppedVehicles)
			  if (t._1 == id)
				  return t;
		  return null;
	  }

	  public void removeVehicle(Tuple2<Integer, Integer> v) {
		  this.stoppedVehicles.remove(v);
		  if(!this.checkAccident())
			  this.resetAccident();
	  }
	  
	  public void addVehicle(Tuple2<Integer, Integer> v) {
		  if(this.stoppedVehicles.contains(v))
			  return;
		  this.stoppedVehicles.add(v);
	  }
	  
	  @Override
	  public String toString() {
		  String vId = "";
		  for(Tuple2<Integer, Integer> t : this.stoppedVehicles) {
			  vId += t._1 +",";
		  }
		  double speed = 0;
		  return "CTIME: " + this.cTime+ " NV:"+" STOPPED:"+this.stoppedVehicles.size()+" V:"+vId;
	  }

	public boolean isUpstreamAccident() {
		return upstreamAccident;
	}

	public void setUpstreamAccident(boolean upstreamAccident) {
		this.upstreamAccident = upstreamAccident;
	}
}
