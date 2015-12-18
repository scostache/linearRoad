package org.myorg.lr;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

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
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;

import org.apache.flink.api.common.functions.Partitioner;

import java.util.Comparator;

import com.google.common.base.Optional;


public class LinearRoadFlink {
	
	protected static final int ACCIDENT_TYPE = 1;
	protected static final int ACCIDENT_TYPE_FAKE = 2;
	protected static final int STATS_TYPE = 3;
	protected static final int POSITION_TYPE = 0;
	protected static final int POSITION_TYPE_FAKE = 4;
	protected static final int ACCIDENT_FW_TYPE = 5;
	
	protected static final int ACCIDENT_FW_FAKE_TYPE = 6;
	
	protected static final int NSEGMENTS_ADVANCE = 4;
	
	protected static int currentPartitions = 1;
	
	protected static final int TRUE = 1;
	protected static final int FALSE = 1;
	
	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("USAGE:hosts file");
			return;
		}

		File fin = new File(args[0]);
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(fin));
		} catch (FileNotFoundException e) {
			System.out.println("Exception in opening receiver host file");
			System.exit(1);
		}
		String line = null;
		ArrayList<Tuple2<String, Integer>> recHosts = new ArrayList<Tuple2<String, Integer>>();
		try {
			while ((line = br.readLine()) != null) {
				String[] elems = line.split(" ");
				if (elems.length < 2) {
					br.close();
					System.out.println("Incompatible receiver host file");
					System.exit(1);
				}
				Tuple2<String, Integer> recHost = new Tuple2<String, Integer>(elems[0], Integer.parseInt(elems[1]));
				recHosts.add(recHost);
			}
		} catch (IOException e) {
			System.out.println("Exception in reading receiver host file");
			System.exit(1);
			e.printStackTrace();
		}
		
		// set up the execution environment
		final StreamExecutionEnvironment ssc = StreamExecutionEnvironment.getExecutionEnvironment();
		
		LinearRoadFlink.currentPartitions = ssc.getParallelism();
		
		ArrayList<DataStream<LRTuple>> arr = new ArrayList<DataStream<LRTuple>>();
		for (Tuple2<String, Integer> t : recHosts) {
			DataStream<LRTuple> initTuples = parseInitialTuples(ssc, t.f0, t.f1);
			arr.add(initTuples);
		}
		DataStream<LRTuple> first = arr.get(0);
		arr.remove(0);
		DataStream<LRTuple> all = first;
		for(DataStream<LRTuple> t: arr)
			all = all.union(t);
		// get input data
		DataStream<LRTuple> initPositionStream = generatePositionStream(all);
		DataStream<LRTuple> positionStream = initPositionStream.
				partitionCustom(new Partitioner4Segments(), new KeyExtractor());
		// we detect if vehicles produced an accident : seg_id, accident=true/false, is/wasAccident=true/false, time
		DataStream<Tuple4<String, Integer, Long, Tuple3<Long,Integer,Integer>>> accidents = positionStream.
				flatMap(new DetectAccidents()).
				partitionCustom(new Partitioner4SegmentsString(), 0);
		//accidents.writeAsCsv("/home/hduser/accFlink.cvs", WriteMode.OVERWRITE);
		// generates stream of vehicles which can be new or not
		DataStream<Tuple7<String, Integer, Long, Long, Long, Long, Optional<LRTuple>>> vidStream = 
				generateVidStream(positionStream);
		//vidStream.writeAsCsv("/home/hduser/newVehiclesFlink.cvs", WriteMode.OVERWRITE);
		// compute the statistics per segment like average speed and number of vehicles
		DataStream<Tuple4<String, Integer, Long, Tuple3<Long, Integer, Integer>>> segstatsStream = 
				generateSegStatsStream(positionStream);
		//segstatsStream.writeAsCsv("/home/hduser/segstatsStreamFlink.cvs", WriteMode.OVERWRITE);
		DataStream<Tuple4<String, Integer, Long, Tuple3<Long, Integer, Integer>>> accStatsStream = accidents.
					union(segstatsStream).
					flatMap(new AccStatsMerger()).partitionCustom(new Partitioner4SegmentsString(), 0);
		//accStatsStream.writeAsCsv("/home/hduser/accStatsFlink.cvs", WriteMode.OVERWRITE);
		DataStream<Tuple7<String, Integer, Long, Long, Long, Long, Optional<LRTuple>>> accTolls = accStatsStream.
					flatMap(new ComputeTolls()).
		    		partitionCustom(new Partitioner4SegmentsString(), 0);
		//accTolls.writeAsCsv("/home/hduser/accTollsFlink.cvs", WriteMode.OVERWRITE);
		// this goes in the next operator which updates accidents, computes tolls and notifies
		// incomming vehicles
		DataStream<Tuple7<String, Integer,Long, Long, Long, Long, Optional<LRTuple>>> accTollsVids = 
				accTolls.union(vidStream).flatMap(new AccVidMerger()).partitionCustom(new Partitioner4SegmentsString(), 0);
		//accTollsVids.writeAsCsv("/home/hduser/accTollsVids.cvs", WriteMode.OVERWRITE);
		
		DataStream<Tuple8<String, Integer, Integer, Long, Long, Integer, Integer, Integer>> alltuplesTollAccident 
		=	accTollsVids.flatMap(new NotifyAccidentsAndTolls());
		//alltuplesTollAccident.writeAsCsv("/home/hduser/alltuplesTollAccident.cvs", WriteMode.OVERWRITE);
		// filter accidents
		DataStream<Tuple6<Integer, Integer, Long, Long, Integer, Integer>> accidentTuples =
				alltuplesTollAccident.filter(
						new FilterFunction<Tuple8<String, Integer, Integer, Long, Long, Integer, Integer, Integer>>() {
							@Override
							public boolean filter(Tuple8<String, Integer, Integer, Long, Long, Integer, Integer, Integer> value)
									throws Exception {
								if(value.f1 == 1)
									return true;
								return false;
							}
						}).map(new MapFunction<Tuple8<String, Integer, Integer, Long, Long, Integer, Integer, Integer>,
								Tuple6<Integer, Integer, Long, Long, Integer, Integer>>() {
									@Override
									public Tuple6<Integer, Integer, Long, Long, Integer, Integer> map(
											Tuple8<String, Integer, Integer, Long, Long, Integer, Integer, Integer> value)
													throws Exception {
										long timec = System.currentTimeMillis();
										return new Tuple6<Integer, Integer, Long, Long, Integer, Integer>(
												value.f1, value.f2, value.f3, timec, value.f5, value.f6);
									}
						});
		DataStream<Tuple6<Integer, Integer, Long, Long, Integer, Integer>> tollTuples = 
				alltuplesTollAccident.filter(new 
						FilterFunction<Tuple8<String, Integer, Integer, Long, Long, Integer, Integer, Integer>>() {
					@Override
					public boolean filter(Tuple8<String, Integer, Integer, Long, Long, Integer, Integer, Integer> value)
							throws Exception {
						if(value.f1 == 0)
							return true;
						return false;
					}
				}).map(new MapFunction<Tuple8<String, Integer, Integer, Long, Long, Integer, Integer, Integer>,
						Tuple6<Integer, Integer, Long, Long, Integer, Integer>>() {
					@Override
					public Tuple6<Integer, Integer, Long, Long, Integer, Integer> map(
							Tuple8<String, Integer, Integer, Long, Long, Integer, Integer, Integer> value)
									throws Exception {
						long timec = System.currentTimeMillis();
						return new Tuple6<Integer, Integer, Long, Long, Integer, Integer>(
								value.f1, value.f2, value.f3, timec, value.f6, value.f7.intValue());
					}
		});
		accidentTuples.writeAsCsv("accidentTuplesFlink.cvs", WriteMode.OVERWRITE);
		tollTuples.writeAsCsv("tollTuplesFlink.cvs", WriteMode.OVERWRITE); 
		// execute program
		ssc.execute("LinearRoad for Flink");
	}

	//
	// User Functions
	//
	public static DataStream<LRTuple> parseInitialTuples(StreamExecutionEnvironment ssc, 
			String host, Integer port) {
		DataStream<String> lines = ssc.socketTextStream(host, port);
		return lines.map(new MapFunction<String, LRTuple>() {
			@Override
			public LRTuple map(String value) throws Exception {
				return new LRTuple(value);
			}
		});
	}

	public static DataStream<LRTuple> generatePositionStream(DataStream<LRTuple> tupleDstream) {
		return tupleDstream.filter(new FilterFunction<LRTuple>() {
			private static final long serialVersionUID = 1L;
			@Override
			public boolean filter(LRTuple x) {
				if (x.type == 0) {
					return true;
				}
				return false;
			}
		});
	}
	
	public static DataStream<Tuple7<String, Integer,Long, Long,Long,Long, Optional<LRTuple>>> 
		generateVidStream(DataStream<LRTuple> positionStream) {
			// prepare for checking accidents in segment
			// here I repartition the data
			// seg_id, tuple type, ? ? LRTuple, integer: is new (1) or not (0)
			return positionStream.flatMap(new DetectNewVehicles()).
					partitionCustom(new Partitioner4SegmentsString(), 0);
	}

	private static DataStream<Tuple4<String, Integer, Long, Tuple3<Long,Integer,Integer>>> 
		generateSegStatsStream(
			DataStream<LRTuple> positionStream) {
		return positionStream.flatMap(new UpdateSegmentsStats()).
				partitionCustom(new Partitioner4SegmentsString(), 0);
	}
	
	
	private static class AccStatsMerger extends RichFlatMapFunction<Tuple4<String, Integer, Long, Tuple3<Long, Integer, Integer>>,
	Tuple4<String, Integer, Long, Tuple3<Long, Integer, Integer>>> implements Checkpointed {
		private Merger<Tuple> merger;

		@Override
		public Serializable snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return null;
		}

		@Override
		public void restoreState(Serializable state) throws Exception {
		}

		@Override
		public void flatMap(Tuple4<String, Integer, Long, Tuple3<Long, Integer, Integer>> value,
				Collector<Tuple4<String, Integer, Long, Tuple3<Long, Integer, Integer>>> out) throws Exception {
			switch(value.f1) {
			case LinearRoadFlink.STATS_TYPE:
				merger.bufferQueue2(value.f0, value);
				break;
			case LinearRoadFlink.ACCIDENT_TYPE:
			case LinearRoadFlink.ACCIDENT_TYPE_FAKE:
				merger.bufferQueue1(value.f0, value);
				break;
			default:
				break;
			}
			List<Tuple> readyTuples = merger.getReadyTuples(value.f0);
			for(Tuple t: readyTuples) {
				out.collect((Tuple4<String, Integer, Long, Tuple3<Long, Integer, Integer>>) t);
			}
		}
		
		@Override
		public void open(Configuration parameters) throws Exception {
			merger = new Merger(new ComparatorAccStats());
		}
	}
	
	private static class AccVidMerger extends 
		RichFlatMapFunction<Tuple7<String, Integer, Long, Long, Long, Long, Optional<LRTuple>>,
		Tuple7<String, Integer, Long, Long, Long, Long, Optional<LRTuple>>> implements Checkpointed {
		private Merger<Tuple> merger;
		private Merger<Tuple> fakemerger;
		
		@Override
		public void open(Configuration parameters) throws Exception {
			merger = new Merger(new ComparatorAccVid());
			fakemerger = new Merger(new ComparatorAccVid());
		}

		@Override
		public Serializable snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			
			return null;
		}

		@Override
		public void restoreState(Serializable state) throws Exception {
			
		}

		@Override
		public void flatMap(Tuple7<String, Integer, Long, Long, Long, Long, Optional<LRTuple>> value,
				Collector<Tuple7<String, Integer, Long, Long, Long, Long, Optional<LRTuple>>> out) throws Exception {
			if(value.f1 == LinearRoadFlink.POSITION_TYPE) {
				merger.bufferQueue2(value.f0, value);
				//we put one for accident and one for toll 
				Tuple7<String, Integer, Long, Long, Long, Long, Optional<LRTuple>> copyvalue = value.copy();
				copyvalue.f1 = LinearRoadFlink.POSITION_TYPE_FAKE;
				merger.bufferQueue2(value.f0, copyvalue);
			} else if (value.f1 == LinearRoadFlink.POSITION_TYPE_FAKE) {
				// we put one for accident from another segment
				merger.bufferQueue2(value.f0, value);
			} else if (value.f1 == LinearRoadFlink.ACCIDENT_FW_TYPE || 
					value.f1 == LinearRoadFlink.ACCIDENT_FW_FAKE_TYPE) {
				merger.bufferQueue1(value.f0, value);
			} else {
				// we store the accidents and the toll tuples
				merger.bufferQueue1(value.f0, value);
			}
			List<Tuple> readyTuples = merger.getReadyTuples(value.f0);
			Iterator<Tuple> it = readyTuples.iterator();
			while(it.hasNext()) {
				Tuple7<String, Integer, Long, Long, Long, Long, Optional<LRTuple>> tnext = 
						(Tuple7<String, Integer, Long, Long, Long, Long, Optional<LRTuple>>) it.next();
				if(tnext.f1 == LinearRoadFlink.POSITION_TYPE_FAKE || 
						tnext.f1 == LinearRoadFlink.ACCIDENT_TYPE_FAKE)
					continue;
				out.collect((Tuple7<String, Integer, Long, Long, Long, Long, Optional<LRTuple>>) tnext);
			}
		}
	}
	
	private static class ComputeTolls extends
	RichFlatMapFunction<Tuple4<String, Integer, Long,Tuple3<Long,Integer,Integer>>, 
		Tuple7<String,Integer,Long, Long, Long, Long, Optional<LRTuple>>>
	implements Checkpointed {
		ComputeTollOperator op;
		
	@Override
	public void flatMap(Tuple4<String, Integer, Long,Tuple3<Long, Integer,Integer>> arg0, 
			Collector<Tuple7<String, Integer,Long, Long, Long, Long, Optional<LRTuple>>> arg1)
			throws Exception {
			op.createIfNotExist(arg0.f0);
			if(arg0.f1 == LinearRoadFlink.ACCIDENT_TYPE || arg0.f1 == LinearRoadFlink.ACCIDENT_TYPE_FAKE) {
				int type = LinearRoadFlink.ACCIDENT_TYPE_FAKE;
				Tuple3<Long, Integer, Integer> value = (Tuple3<Long, Integer, Integer>) arg0.f3;
				if(value.f2 == LinearRoadFlink.TRUE) {
					op.markAndClearAccidents(arg0.f0, value);
					type = LinearRoadFlink.ACCIDENT_TYPE;
				}
				//accident tuple - segment, type 1, timestamp, timeAcc, timeClear
				arg1.collect(new Tuple7<String, Integer, Long, Long, Long, Long, Optional<LRTuple>>(arg0.f0, type,
					arg0.f2,
					value.f0,
					op.getAccidentInfo(arg0.f0).f0,
					op.getAccidentInfo(arg0.f0).f1,
					Optional.fromNullable(((LRTuple) null))));
			} else if (arg0.f1 == LinearRoadFlink.STATS_TYPE) {
				Tuple3<Long, Integer, Integer> value = (Tuple3<Long, Integer, Integer>) arg0.f3;
				boolean hasAccident = false;
				op.setLavNov(arg0.f0, value.f0, value.f1, value.f2);
				if(op.needToOutputAccident(arg0.f0, arg0.f3.f0))
					hasAccident = true;
				long toll = 0;
				if(!hasAccident) {
					op.computeTolls(arg0.f0);
					toll = op.getCurrentToll(arg0.f0, false).longValue();
				}
				arg1.collect(new Tuple7<String, Integer,Long, Long, Long, 
						Long, Optional<LRTuple>>(arg0.f0, LinearRoadFlink.STATS_TYPE, arg0.f2,
								value.f0,
						(long)toll, (long) op.getAverageSpeed(arg0.f0), 
						Optional.fromNullable(((LRTuple) null))));
			}
	}
	
	@Override
	public void open(Configuration parameters) throws Exception {
		op = new ComputeTollOperator();
	}

	@Override
	public void restoreState(Serializable arg0) throws Exception {
		op = (ComputeTollOperator) op;
	}

	@Override
	public Serializable snapshotState(long arg0, long arg1) throws Exception {
		return op;
	}
}

	private static class DetectNewVehicles extends RichFlatMapFunction<LRTuple, 
		Tuple7<String, Integer,Long,Long,Long,Long,Optional<LRTuple>>>
		implements Checkpointed {
		private HashMap<String, NewVehicleState> previousSegments;
		private static int sharedCount = 0;
		
		@Override
		public void flatMap(LRTuple value, 
				Collector<Tuple7<String, Integer, Long, Long, Long, Long, Optional<LRTuple>>> collector) throws Exception {
			String csegment = ""+value.xway+"_"+value.seg;
			boolean isnew = true;
			NewVehicleState n = null;
			if(previousSegments.containsKey(csegment)) {
				n = previousSegments.get(csegment);
				if(n.containsVehicle(value.vid)) {
					isnew = false;
				}
			} else {
				n = new NewVehicleState();
				previousSegments.put(csegment, n);
			}
			n.addVehicle(value.vid, value.simulated_time);
			/*
			if(sharedCount > 50 && n.getHighestTime() > 120000) {
				sharedCount = 0;
				previousSegments.get(csegment).removeVehicles(n.getHighestTime()- 60000);
			} else {
				   sharedCount++;
			}*/
			value.setNew(isnew);
			Tuple7<String, Integer,Long,Long,Long,Long,Optional<LRTuple>> t = 
					new Tuple7<String, Integer,Long,Long,Long,Long,Optional<LRTuple>>(csegment, 
							LinearRoadFlink.POSITION_TYPE, value.time, 
							value.simulated_time, (long)0, (long)0,  Optional.of(value));
			collector.collect(t);
			
			Partitioner4Segments part = new Partitioner4Segments();
			int cpart = part.partition(new Tuple2<Integer, Integer>(value.xway, value.seg), LinearRoadFlink.currentPartitions);
			for(int i = 0; i<= LinearRoadFlink.NSEGMENTS_ADVANCE; i++) {
				Integer csegmentI = value.seg-i;
				if(csegmentI <0)
					break;
				int nextpart = part.partition(new Tuple2<Integer,Integer>(value.xway, csegmentI), LinearRoadFlink.currentPartitions);
				if(cpart != nextpart)  {
					value.seg = csegmentI;
					Tuple7<String, Integer,Long,Long,Long,Long,Optional<LRTuple>> tfake = 
							new Tuple7<String, Integer,Long,Long,Long,Long,Optional<LRTuple>>(csegment, 
									LinearRoadFlink.POSITION_TYPE_FAKE, value.time, 
									(long)0, (long)0, (long)0,  Optional.of(value));
					collector.collect(tfake);
				}
			}
		}
		
		@Override
		public void open(Configuration parameters) throws Exception {
			previousSegments = new HashMap<String, NewVehicleState>();
		}

		@Override
		public Serializable snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return previousSegments;
		}

		@Override
		public void restoreState(Serializable state) throws Exception {
			this.previousSegments = (HashMap<String, NewVehicleState>) state;
		}
	}
	
	private static class UpdateSegmentsStats extends RichFlatMapFunction<LRTuple, 
		Tuple4<String, Integer, Long, Tuple3<Long,Integer,Integer>>>
	implements Checkpointed<ComputeStatsOperator> {
		ComputeStatsOperator op;
		@Override
		public void flatMap(LRTuple tuple, Collector<Tuple4 <String, Integer, Long,
				Tuple3<Long, Integer, Integer>>> collector) throws Exception {
			LRTuple val = tuple;
			String segment = val.xway+"_"+val.seg;
			op.computeStats(segment, val);
			Tuple4<String, Integer, Long, Tuple3<Long, Integer, Integer>> info = 
					new Tuple4<String, Integer, Long, Tuple3<Long, Integer, Integer>>(segment,
					LinearRoadFlink.STATS_TYPE, val.time,
					new Tuple3<Long, Integer, Integer> (val.simulated_time, 
					(int) op.getCurrentAvgSpeed(segment), 
					op.getCurrentNov(segment)));
			collector.collect(info);
		}
		
		@Override
		public void open(Configuration parameters) throws Exception {
			op = new ComputeStatsOperator();
		}

		@Override
		public void restoreState(ComputeStatsOperator arg0) throws Exception {
			op = (ComputeStatsOperator) arg0;
		}

		@Override
		public ComputeStatsOperator snapshotState(long arg0, long arg1) throws Exception {
			return op;
		}
	}

	
	private static class DetectAccidents extends RichFlatMapFunction<LRTuple, 
		Tuple4<String, Integer, Long, Tuple3<Long,Integer,Integer>>> 
		implements Checkpointed<DetectAccidentOperator>{
		private static final long serialVersionUID = 1888474626L;
		private DetectAccidentOperator op;
		
		@Override
		public void flatMap(LRTuple val, Collector<Tuple4<String, Integer, Long, Tuple3<Long,Integer,Integer>>> out) throws Exception {
			ArrayList<Tuple4<String, Integer, Long, Tuple3<Long,Integer,Integer>>> outarr = 
					new ArrayList<Tuple4<String, Integer, Long, Tuple3<Long,Integer,Integer>>>();
			op.run(val.xway, val.seg, val.time, val.simulated_time, val.vid, val.speed, val.lane, val.pos, outarr);
			for(Tuple4<String, Integer, Long, Tuple3<Long,Integer,Integer>> elem : outarr) {
				out.collect(elem);
			}
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			op = new DetectAccidentOperator();
		}

		@Override
		public DetectAccidentOperator snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return op;
		}

		@Override
		public void restoreState(DetectAccidentOperator state) throws Exception {
			this.op = state;
		}

	}

	private static class NotifyAccidentsAndTolls extends
			RichFlatMapFunction<Tuple7<String, Integer, Long, Long,Long,Long, Optional<LRTuple>>,
			Tuple8<String,Integer, Integer, Long, Long, Integer, Integer, Integer>> 
	implements Checkpointed<HashMap<String, Tuple4<Integer, Integer, Long, Long>>> {
		private static final long serialVersionUID = 1888474626L;
		private HashMap<String, Tuple4<Integer, Integer, Long,Long>> segmentStats;
		
		@Override
		public void open(Configuration parameters) throws Exception {
			segmentStats = new HashMap<String, Tuple4<Integer, Integer, Long,Long>>();
		}

		@Override
		public void flatMap(Tuple7<String, Integer, Long,Long,Long,Long, Optional<LRTuple>> v, 
				Collector<Tuple8<String, Integer, Integer, Long, Long, Integer, Integer, Integer>> out)
				throws Exception {
					if(v.f1 == LinearRoadFlink.POSITION_TYPE_FAKE) {
						return;
					}
					if(v.f1 == LinearRoadFlink.POSITION_TYPE) {
						if(!v.f6.isPresent()) {
							System.out.println("LRTUple for segment "+v.f0+" is null " + v.toString());
							return;
						}
						LRTuple val = v.f6.get();
						// to update the tolls
						boolean isChanged = val.isNew();
						if(!isChanged)
							return;
						String segid = val.xway + "_" + val.seg;
						boolean outputAcc = false;
						int toll = 0;
						int speed = 0;
						System.out.println("Checking position tuple : "+val.toString());
						if(segmentStats.containsKey(segid)) {
							Tuple4<Integer, Integer, Long,Long> t = segmentStats.get(segid);
							toll = t.f0;
							speed = t.f1;
							outputAcc = TollSegmentState.needToOutputAccident(val.simulated_time, t.f2, t.f3);
							if(val.lane == 4 && outputAcc) {
								System.out.println("Vehicle is on exit lane!");
								outputAcc = false;
							}
						} else {
							System.out.println("Data structure does not contain segment " + segid);
						}
						if(outputAcc) {
							System.out.println("Collecting accident tuple!");
							out.collect(new Tuple8<String, Integer, Integer, Long, Long, Integer, Integer,Integer>
								(v.f0, 1, val.vid, val.time, System.currentTimeMillis(), val.seg, val.pos,0));
						}
						System.out.println("Collecting toll tuple!");
						out.collect(new Tuple8<String, Integer, Integer, Long, Long, Integer, Integer,Integer>
							(v.f0, 0, val.vid, val.time, System.currentTimeMillis(), val.seg, speed, toll));
					} else {
						Tuple4<Integer, Integer, Long, Long> tuple = null;
						if(segmentStats.containsKey(v.f0)) {
							tuple = segmentStats.get(v.f0);
						} else {
							tuple = new Tuple4<Integer, Integer, Long, Long>(0, 0, (long)0, (long)0);
						}
						if(v.f1 == LinearRoadFlink.ACCIDENT_TYPE) {
							// update accident
							tuple.f2 = v.f4;
							tuple.f3 = v.f5;
							segmentStats.put(v.f0, tuple);
							String[] tmp = v.f0.split("_");
							int segment = Integer.parseInt(tmp[1]);
							String xway = tmp[0];
							// update for previous segments in this partition
							for (int i = 1; i <= LinearRoadFlink.NSEGMENTS_ADVANCE; i++) {
								int realseg = segment - i;
								if (realseg < 0)
									break;
								String key = xway + "_" + realseg;
								if(segmentStats.containsKey(key)) {
									tuple = segmentStats.get(key);
								} else {
									tuple = new Tuple4<Integer, Integer, Long, Long>(0, 0, (long)0, (long)0);
								}
								tuple.f2 = v.f4;
								tuple.f3 = v.f5;
								segmentStats.put(key, tuple);
							}
						} else if(v.f1 == LinearRoadFlink.ACCIDENT_FW_TYPE) {
							tuple.f2 = v.f4;
							tuple.f3 = v.f5;
							segmentStats.put(v.f0, tuple);
							
						} else if (v.f1 == LinearRoadFlink.STATS_TYPE) {
							// update toll and average speed
							tuple.f0 = v.f4.intValue();
							tuple.f1 = v.f5.intValue();
							segmentStats.put(v.f0, tuple);
						}
					}
		}
			

		@Override
		public HashMap<String, Tuple4<Integer, Integer, Long, Long>>
			snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
				return segmentStats;
		}

		@Override
		public void restoreState(HashMap<String, Tuple4<Integer, Integer, Long, Long>> state) throws Exception {
			this.segmentStats = (HashMap<String, Tuple4<Integer, Integer, Long, Long>>) state;
			
		}
		
	}


	public static class KeyExtractor implements KeySelector<LRTuple, Tuple2<Integer,Integer>> {
		@Override
		@SuppressWarnings("unchecked")
		public Tuple2<Integer,Integer> getKey(LRTuple value) {
			// we use xway and direction to partition the stream
			return new Tuple2<Integer, Integer>(value.xway, value.seg);
		}
	}
	
	public static class KeyExtractorString implements KeySelector<Tuple3<String,Integer,Tuple3<Long,Integer,Integer>>, Tuple2<Integer,Integer>> {
		@Override
		@SuppressWarnings("unchecked")
		public Tuple2<Integer,Integer> getKey(Tuple3<String,Integer,Tuple3<Long,Integer,Integer>> value) {
			// we use xway and direction to partition the stream
			String[] values = value.f0.split("_");
			return new Tuple2<Integer, Integer>(Integer.parseInt(values[0]), Integer.parseInt(values[1]));
		}
	}
	
	public static class KeyExtractorTuple implements KeySelector<Tuple2<LRTuple, Boolean>, Tuple2<Integer,Integer>> {
		@Override
		@SuppressWarnings("unchecked")
		public Tuple2<Integer, Integer> getKey(Tuple2<LRTuple, Boolean> value) {
			// we use xway and direction to partition the stream
			return new Tuple2<Integer, Integer>(value.f0.xway, value.f0.seg);
		}
	}
	
	public static class Key4TupleSelector implements KeySelector<Tuple4<String, Long, Integer, Integer>, String> {
		@Override
		@SuppressWarnings("unchecked")
		public String getKey(Tuple4<String, Long, Integer, Integer> value) {
			// we use xway and direction to partition the stream
			return value.f0;
		}
	}
	
	public static class Partitioner4Segments implements Partitioner<Tuple2<Integer, Integer>> {
		@Override
		public int partition(Tuple2<Integer,Integer> key, int numPartitions) {
			return (int) Math.ceil((key.f0 + (int) Math.ceil(key.f1/4)) % numPartitions);
		}
	}
	
	public static class Partitioner4SegmentsString implements Partitioner<String> {
		@Override
		public int partition(String key, int numPartitions) {
			String[] keysplit = key.split("_");
			return (int) Math.ceil((Integer.parseInt(keysplit[0]) + (int) Math.ceil(Integer.parseInt(keysplit[1])/4)) % numPartitions);
		}
	}
}
