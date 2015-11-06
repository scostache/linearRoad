package org.myorg.lr;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FilterFunction;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.RichFlatMapFunction;


public class LinearRoadFlink {

	private static final Pattern SPACE = Pattern.compile(",");
	protected static final int windowAverage = 360; // 5 minutes
	protected static final int windowSlide = 60;
	protected static final int windowCount = 120;
	
	protected static final int HistorySize = 5;


	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>");
			return;
		}

		String hostName = args[0];
		Integer port = Integer.parseInt(args[1]);

		// set up the execution environment
		final StreamExecutionEnvironment ssc = StreamExecutionEnvironment.getExecutionEnvironment();

		// ssc.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		// get input data
		DataStream<String> initTuples = parseInitialTuples(ssc, args[0], args[1]);
		DataStream<String> positionStream = generatePositionStream(initTuples).partitionByHash(new KeyExtractor());
		// we detect if vehicles are stopped or not
		DataStream<Tuple3<String, Boolean, Long>> accidents = positionStream.flatMap(new DetectAccidents());
		// this goes in the next operator which updates accidents and notifies
		// incomming vehicles
		DataStream<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>> alltuplesTollAccident = 
				positionStream.connect(accidents).flatMap(new NotifyAccidentsAndTolls());
		
		// filter accidents
		DataStream<Tuple6<Integer, Integer, Long, Long, Integer, Integer>> accidentTuples =
				alltuplesTollAccident.filter(
						new FilterFunction<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>>() {
							@Override
							public boolean filter(Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double> value)
									throws Exception {
								if(value.f0 == 1)
									return true;
								return false;
							}
							
						}).map(new MapFunction<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>,
								Tuple6<Integer, Integer, Long, Long, Integer, Integer>>() {
									@Override
									public Tuple6<Integer, Integer, Long, Long, Integer, Integer> map(
											Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double> value)
													throws Exception {
										return new Tuple6(value.f0, value.f1, value.f2, value.f3, value.f4, value.f5);
									}
						});
		DataStream<Tuple6<Integer, Integer, Long, Long, Integer, Integer>> tollTuples = 
				alltuplesTollAccident.filter(new FilterFunction<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>>() {
					@Override
					public boolean filter(Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double> value)
							throws Exception {
						if(value.f0 == 1)
							return true;
						return false;
					}
				}).map(new MapFunction<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>,
						Tuple6<Integer, Integer, Long, Long, Integer, Integer>>() {
					@Override
					public Tuple6<Integer, Integer, Long, Long, Integer, Integer> map(
							Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double> value)
									throws Exception {
						return new Tuple6(value.f0, value.f1, value.f2, value.f3, value.f5, value.f6);
					}
		});
		
		accidentTuples.writeAsCsv("accidentTuples.log", WriteMode.OVERWRITE);
		tollTuples.writeAsCsv("tollTuples.log", WriteMode.OVERWRITE);

		// execute program
		ssc.execute("WordCount from SocketTextStream Example");
	}

	//
	// User Functions
	//

	public static DataStream<String> parseInitialTuples(StreamExecutionEnvironment ssc, String host, String port) {
		DataStream<String> lines = ssc.socketTextStream(host, Integer.parseInt(port));
		return lines;
	}

	public static DataStream<String> generatePositionStream(DataStream<String> tupleDstream) {
		return tupleDstream.filter(new FilterFunction<String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public boolean filter(String x) {
				String[] fields = x.split("\\s+");
				byte typeField = Byte.parseByte(fields[0]);

				if (typeField == 0) {
					return true;
				}
				return false;
			}
		});
	}
	
	private static Long getMinute(Long time) {
		return (long) (Math.ceil(time/60) + 1);
	}

	private static class DetectAccidents extends RichFlatMapFunction<String, Tuple3<String, Boolean, Long>> {
		private static final long serialVersionUID = 1888474626L;
		private HashMap<Integer, Vehicle> vehicles;
		private HashMap<String, ArrayList<Vehicle>> segment_stopped;
		private HashMap<String, ArrayList<Integer>> segments;
		private ArrayList<String> have_accidents;

		@Override
		public void flatMap(String value, Collector<Tuple3<String, Boolean, Long>> out) throws Exception {
			// check if vehicle is stopped or not
			String val = (String) value;
			String[] fields = val.split("\\s+");
			int vid = Integer.parseInt(fields[2]);
			long time = Long.parseLong(fields[1]);
			int speed = Integer.parseInt(fields[3]);
			int xway = Integer.parseInt(fields[4]);
			int lane = Integer.parseInt(fields[5]);
			int segment = Integer.parseInt(fields[7]);
			int position = Integer.parseInt(fields[8]);

			String segid = fields[4] + "_" + fields[7];
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

			if (v.stopped) {
				// check if it is stopped in the same position as the other
				// vehicles
				if (!segment_stopped.containsKey(segid)) {
					ArrayList<Vehicle> elems = new ArrayList<Vehicle>();
					elems.add(v);
					segment_stopped.put(segid, elems);
				} else {
					ArrayList<Vehicle> vehicles = segment_stopped.get(segid);
					// check if there is an accident
					for (Vehicle elem : vehicles) {
						if (v.pos == elem.pos && v.lane == elem.lane && (v.time - elem.time) <= 120
								&& v.xway == elem.xway) {
							has_accident = true;
							break;
						}
					}
					if (!vehicles.contains(v))
						vehicles.add(v);
				}
			}

			// we output xway_segment, is_accident for all the 4 upstream
			// segments (if is accident)
			// is connected with position stream
			// in the next operator we update an internal state holding the
			// segments_with_accidents
			// and we output accident notification for vehicle
			if(!have_accidents.contains(segid)) {
				have_accidents.add(segid);
				for (int i = 0; i <= 4; i++) {
					int realseg = segment + i;
					if(realseg > 99)
						break;
					out.collect(new Tuple3<String, Boolean, Long>(xway + "_" + realseg, true, time));
				}
			} else {
				out.collect(new Tuple3<String, Boolean, Long>(xway+"_"+segment, false, time));
				if(have_accidents.contains(segid)) {
					have_accidents.remove(segid);
					for (int i = 1; i <= 4; i++) {
						int realseg = segment + i;
						if(realseg > 99)
							break;
						out.collect(new Tuple3<String, Boolean, Long>(xway + "_" + realseg, false, time));
					}
				}
			}
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			vehicles = new HashMap<Integer, Vehicle>();
			segment_stopped = new HashMap<String, ArrayList<Vehicle>>();
			segments = new HashMap<String, ArrayList<Integer>>();
			have_accidents = new ArrayList<String>();
		}

	}

	private static class NotifyAccidentsAndTolls extends
			RichCoFlatMapFunction<String, Tuple3<String, Boolean, Long>, 
			Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>> {
		private static final long serialVersionUID = 1888474626L;
		private HashMap<String, Tuple2<Long, Long>> accident_segments;
		private HashMap<Integer, String> previous_segments;
		private HashMap<String,ArrayList<MinuteStatistics>> last_minutes; // last 5 speed values
		private HashMap<String,NovLav> last_novlav;
		private HashMap<String, Double> segment_tolls;
		
		
		@Override
		public void open(Configuration parameters) throws Exception {
			accident_segments = new HashMap<String, Tuple2<Long, Long>>();
			previous_segments = new HashMap<Integer, String>();
			last_minutes = new HashMap<String,ArrayList<MinuteStatistics>>();
			last_novlav = new HashMap<String,NovLav>();
			segment_tolls = new HashMap<String, Double>();
		}

		@Override
		public void flatMap1(String value, 
				Collector<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>> out)
				throws Exception {
			String val = (String) value;
			String[] fields = val.split("\\s+");
			int vid = Integer.parseInt(fields[2]);
			long time = Long.parseLong(fields[1]);
			int segment = Integer.parseInt(fields[7]);
			int lane = Integer.parseInt(fields[5]);
			int position = Integer.parseInt(fields[8]);
			int speed = Integer.parseInt(fields[3]);
			String segid = fields[4] + "_" + fields[7];
			
			long minute = getMinute(time);
			
			if(!last_minutes.containsKey(segid)) {
				last_minutes.put(segid, new ArrayList<MinuteStatistics>());
			}
			if(!last_novlav.containsKey(segid)) {
				last_novlav.put(segid, new NovLav());
			}
			if(!segment_tolls.containsKey(segid)) {
				segment_tolls.put(segid, 0.0);
			}
			
			if(last_minutes.get(segid).size() == 0) {
				MinuteStatistics newminute = new MinuteStatistics();
				newminute.addVehicleSpeed(vid, speed);
			} else {
				MinuteStatistics lastmin = last_minutes.get(segid).get(last_minutes.size()-1);
				if(lastmin.getTime() == minute)
					lastmin.addVehicleSpeed(vid, speed);
				else {
					MinuteStatistics newlastmin = new MinuteStatistics();
					newlastmin.addVehicleSpeed(vid, speed);
					if(last_minutes.get(segid).size() == LinearRoadFlink.HistorySize)
						last_minutes.get(segid).remove(0);
				}
			}
			
			if(minute > last_novlav.get(segid).getMinute()) {
				// compute stats for last minute
				double total_avg = 0.0;
				if(last_minutes.get(segid).size() > 1) {
					for(MinuteStatistics m : last_minutes.get(segid).subList(0, last_minutes.get(segid).size()-1)) {
						total_avg += m.speedAverage();
					}
				} else {
					total_avg = last_minutes.get(segid).get(0).speedAverage();
				}
				last_novlav.get(segid).setLav(total_avg);
				last_novlav.get(segid).setNov(last_minutes.get(segid).
						get(last_minutes.get(segid).size()-1).vehicleCount());
				last_novlav.get(segid).setMinute(minute);
				
				// compute tolls?
				double toll = 0;
				if(last_novlav.get(segid).getLav() >= 40 || last_novlav.get(segid).getNov() <=50) {
					toll = 0;
				} else {
					toll = 2*(last_novlav.get(segid).getNov() -50)*(last_novlav.get(segid).getNov() -50);
				}
				segment_tolls.put(segid, toll);
			}
			
			// if vehicle remain in the same segment then ignore
			if(previous_segments.containsKey(vid)) {
				if(previous_segments.get(vid) == segid)
					return;
			}
			synchronized(accident_segments) {
				if(accident_segments.containsKey(segid) && lane != 4) {
					Tuple2<Long, Long> timeacc = accident_segments.get(segid);
					// notify vehicles no earlier than the minute following 
					// the minute when the accident occurred
					long minute_vid = getMinute(time);
					long minute_acc = getMinute(timeacc.f0);
					long minute_clear = getMinute(timeacc.f1);
					if (minute_vid > minute_acc && time < minute_clear) {
						out.collect(new Tuple7<Integer, Integer, Long, Long, Integer, Integer,Double>
								(1, vid, time, System.currentTimeMillis()/1000, segment, position,0.0));
					}
				}
			}
			// output tolls
			double vtoll = segment_tolls.get(segid);
			int sspeed = (int) last_novlav.get(segid).getLav();
			out.collect(new Tuple7<Integer, Integer, Long, Long, Integer, Integer,Double>
			(0, vid, time, System.currentTimeMillis()/1000, segment, sspeed, vtoll));
		}
		
		@Override
		public void flatMap2(Tuple3<String, Boolean, Long> value,
				Collector<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>> out) throws Exception {
			synchronized(accident_segments) {
				if(!accident_segments.containsKey(value.f0) && value.f1) {
					// time at which the accident is started
					accident_segments.put(value.f0, new Tuple2<Long, Long>(value.f2, Long.MAX_VALUE));
				}
				else if (accident_segments.containsKey(value.f0) && !value.f1) {
					Tuple2<Long, Long> acc = accident_segments.get(value.f0);
					acc.f1 = value.f2; // time at which the accident is cleared
				}
			}
		}
	}


	public static class KeyExtractor implements KeySelector<String, String> {
		@Override
		@SuppressWarnings("unchecked")
		public String getKey(String value) {
			String[] fields = value.split("\\s+");
			return fields[1] + "_" + fields[2];
		}
	}
}
