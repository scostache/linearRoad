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
	protected static final int windowAverage = 360; // 5 minutes
	protected static final int windowSlide = 60;
	protected static final int windowCount = 120;
	
	protected static final int HistorySize = 6; // last 5 minutes + current one


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
		
		accidents.writeAsCsv("accidentSegments.log", WriteMode.OVERWRITE);
		// this goes in the next operator which updates accidents, computes tolls and notifies
		// incomming vehicles
		DataStream<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>> alltuplesTollAccident = 
				positionStream.connect(accidents).flatMap(new NotifyAccidentsAndTolls());
		//alltuplesTollAccident.print();
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
				String[] fields = x.split(",");
				byte typeField = Byte.parseByte(fields[0]);

				if (typeField == 0) {
					return true;
				}
				return false;
			}
		});
	}
	
	

	private static class DetectAccidents extends RichFlatMapFunction<String, Tuple3<String, Boolean, Long>> {
		private static final long serialVersionUID = 1888474626L;
		private DetectAccidentOperator op;
		
		@Override
		public void flatMap(String val, Collector<Tuple3<String, Boolean, Long>> out) throws Exception {
			ArrayList<Tuple3<String, Boolean, Long>> outarr = new ArrayList<Tuple3<String, Boolean, Long>>();
			String[] fields = val.split(",");
			int vid = Integer.parseInt(fields[2]);
			long time = Long.parseLong(fields[1]);
			int speed = Integer.parseInt(fields[3]);
			int xway = Integer.parseInt(fields[4]);
			int lane = Integer.parseInt(fields[5]);
			int segment = Integer.parseInt(fields[7]);
			int position = Integer.parseInt(fields[8]);

			op.run(xway, segment, time, vid, speed, lane, position, outarr);
			for(Tuple3<String, Boolean, Long> elem : outarr) {
				out.collect(elem);
			}
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			op = new DetectAccidentOperator();
		}

	}

	private static class NotifyAccidentsAndTolls extends
			RichCoFlatMapFunction<String, Tuple3<String, Boolean, Long>, 
			Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>> {
		private static final long serialVersionUID = 1888474626L;
		private OutputAccidentAndToll op;
		
		
		@Override
		public void open(Configuration parameters) throws Exception {
			op = new OutputAccidentAndToll();
		}

		@Override
		public void flatMap1(String value, 
				Collector<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>> out)
				throws Exception {
			String val = (String) value;
			String[] fields = val.split(",");
			int vid = Integer.parseInt(fields[2]);
			long time = Long.parseLong(fields[1]);
			int speed = Integer.parseInt(fields[3]);
			int xway = Integer.parseInt(fields[4]);
			int lane = Integer.parseInt(fields[5]);
			int segment = Integer.parseInt(fields[7]);
			int position = Integer.parseInt(fields[8]);
			String segid = fields[4] + "_" + fields[7];
			// to update the tolls
			op.computeTolls(segid, time, vid, segment, lane, position, speed);
			
			boolean isChanged = op.vehicleChangedSegment(segid, vid);
			if(!isChanged)
				return;
			boolean outputAcc = op.needToOutputAccident(segid, time, lane);
			if(outputAcc) {
				out.collect(new Tuple7<Integer, Integer, Long, Long, Integer, Integer,Double>
				(1, vid, time, System.currentTimeMillis(), segment, position,0.0));
			}
			double vtoll = op.getCurrentToll(segid);
			int sspeed = (int) op.getAverageSpeed(segid);
			out.collect(new Tuple7<Integer, Integer, Long, Long, Integer, Integer,Double>
				(0, vid, time, System.currentTimeMillis(), segment, sspeed, vtoll));
		}
		
		@Override
		public void flatMap2(Tuple3<String, Boolean, Long> value,
				Collector<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>> out) throws Exception {
			op.markAndClearAccidents(value);
		}
	}


	public static class KeyExtractor implements KeySelector<String, String> {
		@Override
		@SuppressWarnings("unchecked")
		public String getKey(String value) {
			// we use xway and direction to partition the stream
			String[] fields = value.split(",");
			return fields[4] + "_" + fields[6];
		}
	}
}
