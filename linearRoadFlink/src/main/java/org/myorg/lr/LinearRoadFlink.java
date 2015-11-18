package org.myorg.lr;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;


import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;


public class LinearRoadFlink {
	protected static final int windowAverage = 360; // 5 minutes
	protected static final int windowSlide = 60;
	protected static final int windowCount = 120;

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
		DataStream<LRTuple> initTuples = parseInitialTuples(ssc, args[0], args[1]);
		DataStream<LRTuple> initPositionStream = generatePositionStream(initTuples);
		DataStream<LRTuple> positionStream = initPositionStream.partitionByHash(new KeyExtractor());
		// we detect if vehicles are stopped or not
		DataStream<Tuple3<String, Boolean, Long>> accidents = positionStream.flatMap(new DetectAccidents()).partitionByHash(0);
		DataStream<Tuple2<LRTuple, Boolean>> vidStream = generateVidStream(initPositionStream);
		vidStream.writeAsCsv("/home/hduser/newVehiclesFlink.cvs", WriteMode.OVERWRITE);
		// this goes in the next operator which updates accidents, computes tolls and notifies
		// incomming vehicles
		DataStream<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>> alltuplesTollAccident = 
				vidStream.partitionByHash(new KeyExtractorTuple()).
				connect(accidents).flatMap(new NotifyAccidentsAndTolls());
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
										return new Tuple6<Integer, Integer, Long, Long, Integer, Integer>(
												value.f0, value.f1, value.f2, value.f3, value.f4, value.f5);
									}
						});
		DataStream<Tuple6<Integer, Integer, Long, Long, Integer, Integer>> tollTuples = 
				alltuplesTollAccident.filter(new FilterFunction<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>>() {
					@Override
					public boolean filter(Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double> value)
							throws Exception {
						if(value.f0 == 0)
							return true;
						return false;
					}
				}).map(new MapFunction<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>,
						Tuple6<Integer, Integer, Long, Long, Integer, Integer>>() {
					@Override
					public Tuple6<Integer, Integer, Long, Long, Integer, Integer> map(
							Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double> value)
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
	public static DataStream<LRTuple> parseInitialTuples(StreamExecutionEnvironment ssc, String host, String port) {
		DataStream<String> lines = ssc.socketTextStream(host, Integer.parseInt(port));
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
		DataStream<Tuple2<LRTuple, Boolean>> vidStream = positionStream.partitionByHash(new KeySelectorByDirXway()).
				map(new DetectNewVehicles());
		return vidStream;
	}
	
	private static class DetectNewVehicles extends RichMapFunction<LRTuple, Tuple2<LRTuple, Boolean>>
		implements Checkpointed {
		private HashMap<Integer, Tuple2<String, Long>> previousSegments;
		private static int count = 0;
		
		@Override
		public Tuple2<LRTuple, Boolean> map(LRTuple value) throws Exception {
			String csegment = ""+value.xway+"_"+value.seg;
			boolean isnew = false;
			if(previousSegments.containsKey(value.vid)) {
				if(!previousSegments.get(value.vid).equals(csegment)) {
					isnew = true;
				}
			} else {
				isnew = true;
			}
			previousSegments.put(value.vid, new Tuple2<String, Long>(csegment, System.currentTimeMillis()));
			if(count == 50) {
				count = 0;
				long time = System.currentTimeMillis();
				ArrayList<Integer> toRemove = new ArrayList<Integer>();
				for(Integer vt : this.previousSegments.keySet()) {
					if(time - this.previousSegments.get(vt).f1 > 30000) {
						toRemove.add(vt);
					}
				}
				for(Integer vt : toRemove)
					previousSegments.remove(vt);
			}
			return new Tuple2<LRTuple, Boolean>(value, isnew);
		}
		
		@Override
		public void open(Configuration parameters) throws Exception {
			previousSegments = new HashMap<Integer, Tuple2<String, Long>>();
		}

		@Override
		public Serializable snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return previousSegments;
		}

		@Override
		public void restoreState(Serializable state) throws Exception {
			this.previousSegments = (HashMap<Integer, Tuple2<String, Long>>) state;
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
			RichCoFlatMapFunction<Tuple2<LRTuple, Boolean>, Tuple3<String, Boolean, Long>, 
			Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>> 
	implements Checkpointed<OutputAccidentAndToll>{
		private static final long serialVersionUID = 1888474626L;
		private OutputAccidentAndToll op;
		
		@Override
		public void open(Configuration parameters) throws Exception {
			op = new OutputAccidentAndToll();
		}

		@Override
		public void flatMap1(Tuple2<LRTuple, Boolean> v, 
				Collector<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>> out)
				throws Exception {
			LRTuple val = v.f0;
			String segid = val.xway + "_" + val.seg;
			// to update the tolls
			op.computeTolls(segid, val.time, val.vid, val.seg, val.lane, val.pos, val.speed);
			boolean isChanged = v.f1;
			if(!isChanged)
				return;
			boolean outputAcc = op.needToOutputAccident(segid, val.time, val.lane);
			if(outputAcc) {
				out.collect(new Tuple7<Integer, Integer, Long, Long, Integer, Integer,Double>
					(1, val.vid, val.time, System.currentTimeMillis(), val.seg, val.pos,0.0));
			}
			double vtoll = op.getCurrentToll(segid, outputAcc);
			int sspeed = (int) op.getAverageSpeed(segid);
			out.collect(new Tuple7<Integer, Integer, Long, Long, Integer, Integer,Double>
				(0, val.vid, val.time, System.currentTimeMillis(), val.seg, sspeed, vtoll));
		}
		
		@Override
		public void flatMap2(Tuple3<String, Boolean, Long> value,
				Collector<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>> out) throws Exception {
			op.markAndClearAccidents(value);
		}

		@Override
		public OutputAccidentAndToll snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return op;
		}

		@Override
		public void restoreState(OutputAccidentAndToll state) throws Exception {
			this.op = state;
			
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
	
	
	public static class KeySelectorByDirXway implements KeySelector<LRTuple, String> {
		@Override
		@SuppressWarnings("unchecked")
		public String getKey(LRTuple value) {
			// we use xway and direction to partition the stream
			return ""+value.xway;
		}
	}
}
