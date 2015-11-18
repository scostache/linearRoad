package linearRoadSpark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;

public class LRTuple implements Serializable{
	private static final Pattern SPACE = Pattern.compile(",");
	// Type = 0, Time, VID, Spd, XWay, Lane, Dir, Seg, Pos
	public int type;
	public long time;
	public int vid;
	public int speed;
	public int xway;
	public int lane;
	public int dir;
	public int seg;
	public int pos;
	
	public LRTuple(String input) {
		ArrayList<String> elems = Lists.newArrayList(SPACE.split(input.trim()));
		type = Integer.parseInt(elems.get(0));
		time = Long.parseLong(elems.get(1));
		vid = Integer.parseInt(elems.get(2));
		speed = Integer.parseInt(elems.get(3));
		xway = Integer.parseInt(elems.get(4));
		lane = Integer.parseInt(elems.get(5));
		dir = Integer.parseInt(elems.get(6));
		seg = Integer.parseInt(elems.get(7));
		pos = Integer.parseInt(elems.get(8));
	}

	@Override
	public String toString() {
		return "type:"+type+" time:"+time+" vid:"+vid+
				" "+speed+" xway:"+xway+" lane:"+lane+
				" dir:"+dir+" seg:"+seg+" pos:"+pos;
	}
}
