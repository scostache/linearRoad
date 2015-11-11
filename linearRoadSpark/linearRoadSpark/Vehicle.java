package linearRoadSpark;

import java.io.Serializable;

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
	  
	  void update(int pos, int xway, int seg, int lane, int speed, long time) {
		  if(pos == this.pos && xway == this.xway && seg == this.seg)
			  this.nsamepos++;
		  else {
			  this.nsamepos = 0;
			  if (this.stopped)
				  this.stopped = false;
		  }
		  if(this.nsamepos == 4)
			  this.stopped = true;
		  
		  this.pos = pos;
		  this.isNew = false;
		  this.time = time;
		  this.lane = lane;
		  this.xway = xway;
		  this.seg = seg;
		  
		  timeLastUpdate = time;
	  }
	  
	  
	  @Override
	  public String toString() {
		  return "("+this.id+","+this.time+","+this.pos+","+seg+")";
	  }
}
