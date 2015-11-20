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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;

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
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
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


public class LinearRoadFlink {
	protected static final int windowAverage = 360; // 5 minutes
	protected static final int windowSlide = 60;
	protected static final int windowCount = 120;

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>");
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
		// ssc.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		// get input data
		DataStream<LRTuple> initPositionStream = generatePositionStream(all);
		DataStream<LRTuple> positionStream = initPositionStream.partitionByHash(new KeyExtractor());
		
		// we detect if vehicles produced an accident
		DataStream<Tuple3<String, Boolean, Long>> accidents = positionStream.flatMap(new DetectAccidents()).partitionByHash(0);
		
		// generates stream of vehicles which can be new or not
		DataStream<Tuple2<LRTuple, Boolean>> vidStream = generateVidStream(positionStream);
		vidStream.writeAsCsv("/home/hduser/newVehiclesFlink.cvs", WriteMode.OVERWRITE);
		// compute the statistics per segment like average speed and number of vehicles
		DataStream<Tuple4<String, Long, Integer, Integer>> segstatsStream = generateSegStatsStream(positionStream);
		//segstatsStream.writeAsCsv("/home/hduser/segstatsStream.cvs", WriteMode.OVERWRITE);
		
		DataStream<Tuple4<String, Integer, Long,Long>> accTolls = accidents.connect(segstatsStream).flatMap(new ComputeTolls());
		accTolls.writeAsCsv("/home/hduser/accTollsFlink.cvs", WriteMode.OVERWRITE);
		
		// this goes in the next operator which updates accidents, computes tolls and notifies
		// incomming vehicles
		DataStream<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Integer>> alltuplesTollAccident = 
				vidStream.partitionByHash(new KeyExtractorTuple()).
				connect(accTolls).flatMap(new NotifyAccidentsAndTolls());
		// filter accidents
		DataStream<Tuple6<Integer, Integer, Long, Long, Integer, Integer>> accidentTuples =
				alltuplesTollAccident.filter(
						new FilterFunction<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Integer>>() {
							@Override
							public boolean filter(Tuple7<Integer, Integer, Long, Long, Integer, Integer, Integer> value)
									throws Exception {
								if(value.f0 == 1)
									return true;
								return false;
							}
						}).map(new MapFunction<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Integer>,
								Tuple6<Integer, Integer, Long, Long, Integer, Integer>>() {
									@Override
									public Tuple6<Integer, Integer, Long, Long, Integer, Integer> map(
											Tuple7<Integer, Integer, Long, Long, Integer, Integer, Integer> value)
													throws Exception {
										return new Tuple6<Integer, Integer, Long, Long, Integer, Integer>(
												value.f0, value.f1, value.f2, value.f3, value.f4, value.f5);
									}
						});
		DataStream<Tuple6<Integer, Integer, Long, Long, Integer, Integer>> tollTuples = 
				alltuplesTollAccident.filter(new FilterFunction<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Integer>>() {
					@Override
					public boolean filter(Tuple7<Integer, Integer, Long, Long, Integer, Integer, Integer> value)
							throws Exception {
						if(value.f0 == 0)
							return true;
						return false;
					}
				}).map(new MapFunction<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Integer>,
						Tuple6<Integer, Integer, Long, Long, Integer, Integer>>() {
					@Override
					public Tuple6<Integer, Integer, Long, Long, Integer, Integer> map(
							Tuple7<Integer, Integer, Long, Long, Integer, Integer, Integer> value)
									throws Exception {
						return new Tuple6<Integer, Integer, Long, Long, Integer, Integer>(
								value.f0, value.f1, value.f2, value.f3, value.f5, value.f6.intValue());
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
	public static DataStream<LRTuple> parseInitialTuples(StreamExecutionEnvironment ssc, String host, Integer port) {
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
	
	public static DataStream<Tuple2<LRTuple, Boolean>> generateVidStream(DataStream<LRTuple> positionStream) {
		// prepare for checking accidents in segment
		// here I repartition the data
		return positionStream.map(new DetectNewVehicles()).partitionByHash(new KeyExtractorTuple());
	}

	private static DataStream<Tuple4<String, Long, Integer, Integer>> generateSegStatsStream(
			DataStream<LRTuple> positionStream) {
		return positionStream.flatMap(new UpdateSegmentsStats()).partitionByHash(new Key4TupleSelector());
	}
	
	
	private static class ComputeTolls extends
	RichCoFlatMapFunction<Tuple3<String, Boolean, Long>, Tuple4<String, Long, Integer, Integer>, Tuple4<String, Integer, Long, Long>>
	implements Checkpointed {
		ComputeTollOperator op;
		
	@Override
	public void flatMap1(Tuple3<String, Boolean, Long> arg0, Collector<Tuple4<String, Integer, Long, Long>> arg1)
			throws Exception {
		// accident tuple - type 1
		op.markAndClearAccidents(arg0);
		//accident tuple - segment, type 1, current_toll, average_speed, accident=true/false
		arg1.collect(new Tuple4<String, Integer, Long, Long>(arg0.f0, 1, 
					op.getAccidentInfo(arg0.f0).f0,
					op.getAccidentInfo(arg0.f0).f1));
	}
	
	@Override
	public void flatMap2(Tuple4<String, Long, Integer, Integer> arg0, Collector<Tuple4<String, Integer, Long, Long>> arg1)
			throws Exception {
		op.setLavNov(arg0.f0, arg0.f1, arg0.f2, arg0.f3);
		op.computeTolls(arg0.f0);
		// toll tuple - xsegment, type 0, current_toll, average_speed, accident=false
		if(op.needToOutput(arg0.f0, arg0.f1)) {
			arg1.collect(new Tuple4<String, Integer, Long, Long>(arg0.f0, 0, 
					op.getCurrentToll(arg0.f0, false).longValue(), (long) op.getAverageSpeed(arg0.f0)));
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
	
	private static class DetectNewVehicles extends RichMapFunction<LRTuple, Tuple2<LRTuple, Boolean>>
		implements Checkpointed {
		private HashMap<String, NewVehicleState> previousSegments;
		private static int sharedCount = 0;
		
		@Override
		public Tuple2<LRTuple, Boolean> map(LRTuple value) throws Exception {
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
			n.addVehicle(value.vid, value.time);
			/*
			if(sharedCount > 50 && n.getHighestTime() > 60000) {
				sharedCount = 0;
				previousSegments.get(csegment).removeVehicles(n.getHighestTime()- 60000);
			} else {
				   sharedCount++;
			}*/
			return new Tuple2<LRTuple, Boolean>(value, isnew);
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
	
	private static class UpdateSegmentsStats extends RichFlatMapFunction<LRTuple, Tuple4<String, Long, Integer, Integer>>
	implements Checkpointed<ComputeStatsOperator> {
		ComputeStatsOperator op;
		@Override
		public void flatMap(LRTuple tuple, Collector<Tuple4<String, Long, Integer, Integer>> collector) throws Exception {
			String segment = tuple.xway+"_"+tuple.seg;
			boolean res = op.needToOutput(segment, tuple);
			//System.out.println("Need to output for tuple "+tuple.toString()+" "+res);
			op.computeStats(segment, tuple);
			if(res) {
				Tuple4<String, Long, Integer, Integer> info = new Tuple4<String, Long, Integer, Integer>(segment, tuple.time, 
						(int) op.getCurrentAvgSpeed(segment), op.getCurrentNov(segment));
				//System.out.println("Outputing "+info);
				collector.collect(info);
			}
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

	

	private static class DetectAccidents extends RichFlatMapFunction<LRTuple, Tuple3<String, Boolean, Long>> 
	implements Checkpointed<DetectAccidentOperator>{
		private static final long serialVersionUID = 1888474626L;
		private DetectAccidentOperator op;
		
		@Override
		public void flatMap(LRTuple val, Collector<Tuple3<String, Boolean, Long>> out) throws Exception {
			ArrayList<Tuple3<String, Boolean, Long>> outarr = new ArrayList<Tuple3<String, Boolean, Long>>();
			op.run(val.xway, val.seg, val.time, val.vid, val.speed, val.lane, val.pos, outarr);
			for(Tuple3<String, Boolean, Long> elem : outarr) {
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
			RichCoFlatMapFunction<Tuple2<LRTuple, Boolean>, Tuple4<String, Integer, Long,Long>, 
			Tuple7<Integer, Integer, Long, Long, Integer, Integer, Integer>> 
	implements Checkpointed<HashMap<String, Tuple4<Integer, Integer, Long, Long>>>{
		private static final long serialVersionUID = 1888474626L;
		private HashMap<String, Tuple4<Integer, Integer, Long,Long>> segmentStats;
		
		@Override
		public void open(Configuration parameters) throws Exception {
			segmentStats = new HashMap<String, Tuple4<Integer, Integer, Long,Long>>();
		}

		@Override
		public void flatMap1(Tuple2<LRTuple, Boolean> v, 
				Collector<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Integer>> out)
				throws Exception {
			LRTuple val = v.f0;
			// to update the tolls
			boolean isChanged = v.f1;
			if(!isChanged)
				return;
			String segid = val.xway + "_" + val.seg;
			boolean outputAcc = false;
			int toll = 0;
			int speed = 0;
			if(segmentStats.containsKey(segid)) {
				Tuple4<Integer, Integer, Long,Long> t = segmentStats.get(segid);
				toll = t.f0;
				speed = t.f1;
				outputAcc = TollSegmentState.needToOutputAccident(v.f0.time, t.f2, t.f3);
				if(v.f0.lane == 4 && outputAcc)
					outputAcc = false;
			}
			if(outputAcc) {
				out.collect(new Tuple7<Integer, Integer, Long, Long, Integer, Integer,Integer>
					(1, val.vid, val.time, System.currentTimeMillis(), val.seg, val.pos,0));
			}
			out.collect(new Tuple7<Integer, Integer, Long, Long, Integer, Integer,Integer>
				(0, val.vid, val.time, System.currentTimeMillis(), val.seg, speed, toll));
		}
		
		@Override
		public void flatMap2(Tuple4<String, Integer, Long,Long> value,
				Collector<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Integer>> out) throws Exception {
			
			Tuple4<Integer, Integer, Long, Long> tuple = null;
			if(segmentStats.containsKey(value.f0))
				tuple = segmentStats.get(value.f0);
			else {
				tuple = new Tuple4<Integer, Integer, Long, Long>(0,0,(long)0,(long)0);
			}
			if(value.f1 == 1) {
				// update accident
				tuple.f2 = value.f2;
				tuple.f3 = value.f3;
			} else {
				// update toll and average speed
				tuple.f0 = value.f2.intValue();
				tuple.f1 = value.f3.intValue();
			}
			segmentStats.put(value.f0, tuple);
		}

		@Override
		public HashMap<String, Tuple4<Integer, Integer, Long, Long>> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return segmentStats;
		}

		@Override
		public void restoreState(HashMap<String, Tuple4<Integer, Integer, Long, Long>> state) throws Exception {
			this.segmentStats = (HashMap<String, Tuple4<Integer, Integer, Long, Long>>) state;
			
		}
	}


	public static class KeyExtractor implements KeySelector<LRTuple, String> {
		@Override
		@SuppressWarnings("unchecked")
		public String getKey(LRTuple value) {
			// we use xway and direction to partition the stream
			return ""+value.xway+"_"+value.seg;
		}
	}
	
	public static class KeyExtractorTuple implements KeySelector<Tuple2<LRTuple, Boolean>, String> {
		@Override
		@SuppressWarnings("unchecked")
		public String getKey(Tuple2<LRTuple, Boolean> value) {
			// we use xway and direction to partition the stream
			return ""+value.f0.xway+"_"+value.f0.seg;
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
	
}
