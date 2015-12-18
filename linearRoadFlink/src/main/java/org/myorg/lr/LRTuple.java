package org.myorg.lr;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.regex.Pattern;


public class LRTuple implements Serializable{
	private static final Pattern SPACE = Pattern.compile(",");
	// Type = 0, Time, VID, Spd, XWay, Lane, Dir, Seg, Pos
	public int type;
	public long simulated_time;
	public long time;
	public int vid;
	public int speed;
	public int xway;
	public int lane;
	public int dir;
	public int seg;
	public int pos;
	private boolean isNew;
	
	public LRTuple(String input) {
		setNew(false);
		String[] elems = SPACE.split(input.trim());
		type = Integer.parseInt(elems[0]);
		time = Long.parseLong(elems[1]);
		vid = Integer.parseInt(elems[2]);
		speed = Integer.parseInt(elems[3]);
		xway = Integer.parseInt(elems[4]);
		lane = Integer.parseInt(elems[5]);
		dir = Integer.parseInt(elems[6]);
		seg = Integer.parseInt(elems[7]);
		pos = Integer.parseInt(elems[8]);
		simulated_time = Integer.parseInt(elems[9]);
	}

	@Override
	public String toString() {
		return "type:"+type+" time:"+time+ " simulated time:"+simulated_time+" vid:"+vid+
				" speed:"+speed+" xway:"+xway+" lane:"+lane+
				" dir:"+dir+" seg:"+seg+" pos:"+pos+" isNew:"+this.isNew;
	}

	public boolean isNew() {
		return isNew;
	}

	public void setNew(boolean isNew) {
		this.isNew = isNew;
	}
}
