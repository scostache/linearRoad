package linearRoadSpark;

import java.io.Serializable;

import org.apache.spark.streaming.Time;

public class Vehicle implements Serializable{
	  public int id;
	  public long time;
	  public int xway;
	  public int lane;
	  public int seg;
	  public int pos;
	  public boolean stopped;
	  public boolean pastStopped;
	  public int nsamepos;
	  public boolean isNew;
	  public long timeNew;
	  public long timeLastUpdate;
	  
	  Vehicle(int id, long time, int xway, int lane, int seg, int pos) {
		  this.id = id;
		  this.time = time;
		  this.xway = xway;
		  this.lane = lane;
		  this.seg = seg;
		  this.pos = pos;
		  this.nsamepos = 0;
		  this.stopped = false;
		  this.pastStopped = false;
		  this.isNew = true;
		  timeNew = time;
		  timeLastUpdate = System.currentTimeMillis()/1000;
	  }
	  
	  void update(int pos, int dir, int lane, int speed, long time) {
		  this.pos = pos;
		  this.isNew = false;
		  this.time = time;
		  this.lane = lane;
		  timeLastUpdate = time;
	  }
	  
	  boolean checkValidity() {
		  long now = System.currentTimeMillis()/1000;
		  if(now - timeLastUpdate > 30)
			  return false;
		  return true;
	  }
	  
	  @Override
	  public String toString() {
		  return "("+this.id+","+this.time+","+this.pos+","+seg+")";
	  }
}
