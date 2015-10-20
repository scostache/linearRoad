package linearRoadSpark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import scala.Function0;
import scala.Tuple2;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import org.apache.log4j.*;
import org.apache.spark.Logging;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class LinearRoad implements Serializable{
	private static final Pattern SPACE = Pattern.compile(",");
	private static final Pattern EOL = Pattern.compile("\n");
	
	private static int checkpointCount;
	private static JavaPairRDD<String, SegmentState> globalSegmentState;
	
	private JavaStreamingContext ssc;

	public LinearRoad(JavaStreamingContext ssc) {
		this.ssc = ssc;
		@SuppressWarnings("unchecked")
		List<Tuple2<String, SegmentState>> tuples = Arrays.asList(
				new Tuple2<String, SegmentState>("1_0", new SegmentState()),
				new Tuple2<String, SegmentState>("1_1", new SegmentState()));
		globalSegmentState = ssc.sc().parallelizePairs(tuples, LinearRoadMain.currentPartitions);
		
		globalSegmentState.cache();

	}


	public static JavaPairDStream<Integer, Tuple2<Long, List<Integer>>> parseInitialTuples(JavaStreamingContext ssc, String host, String port) {
		JavaReceiverInputDStream<String> lines = ssc.socketTextStream(host, Integer.parseInt(port), 
				StorageLevels.MEMORY_AND_DISK);
		return lines.transformToPair(new Function<JavaRDD<String>,
				JavaPairRDD<Integer, Tuple2<Long, List<Integer>>> >() {
					@Override
					public JavaPairRDD<Integer, Tuple2<Long, List<Integer>>> call(JavaRDD<String> v1) throws Exception {
					JavaPairRDD<Integer, Tuple2<Long, List<Integer>>> res = v1.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, Integer, Tuple2<Long, List<Integer>>>() {
					@Override
					public Iterable<Tuple2<Integer, Tuple2<Long, List<Integer>>>> call(Iterator<String> x) {
						// return a tuple identified by key
						// Type = 0, Time, VID, Spd, XWay, Lane, Dir, Seg, Pos
						List<Tuple2<Integer, Tuple2<Long, List<Integer>>>> res = new ArrayList<Tuple2<Integer, Tuple2<Long, List<Integer>>>>();
						Long time = (long) System.currentTimeMillis() / 1000;
						Iterator<String> it = x;
						
						while(it.hasNext()) {
							String elem = it.next();
							ArrayList elems = Lists.newArrayList(SPACE.split(elem.trim()));
							Integer key = Integer.parseInt((String) elems.get(0));
							elems.remove(0);
							ArrayList<Integer> ints = new ArrayList<Integer>();
							for (Object e : elems)
								ints.add(Integer.parseInt((String) e));
							res.add( new Tuple2(key, new Tuple2(time, ints)));
						}
						return res;
					}
				});
					LinearRoad.checkpointCount++;
					if(LinearRoad.checkpointCount > LinearRoadMain.maxCheckpoint)
						LinearRoad.checkpointCount = 0;
					return res.repartition(LinearRoadMain.currentPartitions);
					}
		});
	}

	public static JavaPairDStream<Integer, Tuple2<Long, List<Integer>>> generatePositionStream(
			JavaPairDStream<Integer, Tuple2<Long, List<Integer>>> tupleDstream) {
		JavaPairDStream<Integer, Tuple2<Long, List<Integer>>> positionStream = tupleDstream
				.filter(new Function<Tuple2<Integer, Tuple2<Long, List<Integer>>>, Boolean>() {
					@Override
					public Boolean call(Tuple2<Integer, Tuple2<Long, List<Integer>>> x) {
						if (x._1 == 0)
							return true;
						return false;
					}
				});
		//positionStream.cache();
		return positionStream;
	}

	public static JavaPairDStream<String, Tuple2<Long, List<Integer>>> generateSegmentStream(
			JavaPairDStream<Integer, Tuple2<Long, List<Integer>>> positionStream) {
		// prepare for checking accidents in segment
		// here I repartition the data 
		JavaPairDStream<String, Tuple2<Long, List<Integer>>> segmentStream = positionStream.transformToPair(
				new Function<JavaPairRDD<Integer, Tuple2<Long, List<Integer>>>, 
					JavaPairRDD<String, Tuple2<Long, List<Integer>>>>() {
					@Override
					public JavaPairRDD<String, Tuple2<Long, List<Integer>>> call(
							JavaPairRDD<Integer, Tuple2<Long, List<Integer>>> s) {
						
						JavaPairRDD<String, Tuple2<Long, List<Integer>>> res = s.mapToPair(
								new PairFunction<Tuple2<Integer, Tuple2<Long, List<Integer>>>, 
								String, Tuple2<Long, List<Integer> >>() {
							@Override 
							public Tuple2<String, Tuple2<Long, List<Integer>>> call(
									Tuple2<Integer, Tuple2<Long, List<Integer>>> s1) {
							String seg = s1._2._2.get(3).toString() + '_' + s1._2._2.get(6).toString();
							return new Tuple2<String, Tuple2<Long, List<Integer>>>(seg, s1._2);
							}
							}).repartitionAndSortWithinPartitions(
									new HashPartitioner(LinearRoadMain.currentPartitions));
						res.cache();
						return res;
					}
				});
		return segmentStream;
	}

	public static JavaPairDStream<String, SegmentState> checkAccidents(
			JavaPairDStream<String, SegmentState> data,
			JavaPairDStream<String, Tuple2<Long, List<Integer>>> segmentStream) {
		return data.join(segmentStream).updateStateByKey(updateSegmentState, 
				new HashPartitioner(LinearRoadMain.currentPartitions), globalSegmentState);
				
		/*
				transformToPair(
				new Function2<JavaPairRDD<String, Tuple2<SegmentState, Tuple2<Long, List<Integer>>>>, 
				Time, JavaPairRDD<String, SegmentState>>() {
			@Override
			public JavaPairRDD<String, SegmentState> call(
					JavaPairRDD<String, Tuple2<SegmentState, Tuple2<Long, List<Integer>>>> newbatch, Time v2)
					throws Exception {
				JavaPairRDD<String, SegmentState> newstate = newbatch.mapValues(
					new Function<Tuple2<SegmentState, Tuple2<Long, List<Integer>>>, SegmentState>() {
						@Override
						public SegmentState call(Tuple2<SegmentState, Tuple2<Long, List<Integer>>> v1)
								throws Exception {
							return updateSegment(v1._2, Optional.of(v1._1)).get();
						}
				});
				globalSegmentState.unpersist(false);
				globalSegmentState = newstate.persist(StorageLevel.MEMORY_AND_DISK());
				if (checkpointCount == 0) {
					globalSegmentState.checkpoint();
				}
				//globalSegmentState.take(1);
				return globalSegmentState;
			}

		});	
		*/
	}
	
	public static void outputAccidents(JavaStreamingContext ssc, 
			JavaPairDStream<String, Tuple2<Long, List<Integer>>> segmentStream,
			JavaPairDStream<Integer, Tuple2<Long, List<Integer>>> positionStream,
			JavaPairDStream<String, SegmentState> stateDstream) {
		 // segmentStream.print();
		 computeAccidents(ssc, segmentStream, positionStream, stateDstream). foreachRDD(new
				 Function<JavaPairRDD<String,Tuple2<Tuple2<Tuple2<Long,List<Integer>>, SegmentState>,Boolean >>, Void>() {
		 @Override public Void call(JavaPairRDD<String, Tuple2<Tuple2<Tuple2<Long,List<Integer>>, SegmentState>, Boolean>> v1) throws
		 	Exception { for(Tuple2<String, Tuple2<Tuple2<Tuple2<Long,List<Integer>>, SegmentState>,Boolean>> elem : v1.collect()) { 
		 		List<Integer> s = elem._2._1._1._2; 
		 		long time = elem._2._1._1._1; 
		 		if (((long) s.get(s.size()-1)/60000)+1 >= elem._2._1._2.minuteAccident.intValue() + 1) 
		 			System.out.println("1"+","+s.get(1)+","+time+","+ System.currentTimeMillis()/1000 + ","+s.get(6)+","+s.get(3)); 
		 		}
		 	return null; } });
	}

	

	private static JavaPairDStream<String, Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>,Boolean>> computeAccidents(
			JavaStreamingContext ssc, JavaPairDStream<String, Tuple2<Long, List<Integer>>> segmentStream,
			JavaPairDStream<Integer, Tuple2<Long, List<Integer>>> positionStream,
			JavaPairDStream<String, SegmentState> stateDstream) {
		
		JavaPairDStream<String, Tuple2<Tuple2<Long, List<Integer>>, SegmentState>> currentsegmentStream = segmentStream
				.join(stateDstream);
		JavaPairDStream<String, Boolean> propagatedAccidentStream = propagateAccidents(ssc, currentsegmentStream,
				globalSegmentState);

		return getAccidentSegments(currentsegmentStream, propagatedAccidentStream);
	}

	private static JavaPairDStream<String, Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Boolean>> getAccidentSegments(
			JavaPairDStream<String, Tuple2<Tuple2<Long, List<Integer>>, SegmentState>> currentSegmentStream,
			JavaPairDStream<String, Boolean> propagatedAccidentStream) {
		return currentSegmentStream.join(propagatedAccidentStream)
				.filter(new Function<Tuple2<String, Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Boolean>>,Boolean>() {
					@Override
					public Boolean call(Tuple2<String, Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Boolean>> v1)
							throws Exception {
						return v1._2._2;
					}
				});
	}

	public static JavaPairDStream<String, SegmentState> checkVehiclesLeftSegment(JavaStreamingContext ssc,
			JavaPairDStream<Integer, Tuple2<Long, List<Integer>>> positionStream) {
		JavaPairDStream<String, Tuple2<Integer, Integer>> tmp = positionStream
				.mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Long, List<Integer>>>, Integer, List<Integer>>() {
					@Override
					public Tuple2<Integer, List<Integer>> call(Tuple2<Integer, Tuple2<Long, List<Integer>>> s) {
						ArrayList elems = new ArrayList();
						elems.add(s._2._2.get(0)); // time (0)
						elems.add(s._2._2.get(3)); // xway (1)
						elems.add(s._2._2.get(6)); // position (2)
						elems.add(s._2._2.get(7)); // segment (3)
						elems.add(0); // does vehicle changed segment or not? for reduce
						return new Tuple2(s._2._2.get(1), elems);
					}
				})
				.reduceByKeyAndWindow(
						new Function2<List<Integer>, List<Integer>, List<Integer>>() {
							@Override
							public List<Integer> call(List<Integer> v1, List<Integer> v2) throws Exception {
								if (v1.get(3) == v2.get(3)) {
									if(v1.get(0) > v2.get(0))
										return v1;
									else
										return v2;
								}
								if(v1.get(0) > v2.get(0)) {
									v2.remove(v2.size()-1);
									v2.add(1); // we mark the change in segment
									return v2;
								}
								v1.remove(v1.size()-1);
								v1.add(1); // we mark the change in segment
								return v1;
							}
							
						}, new Duration(60000), LinearRoadMain.windowSlide).
							filter(new Function<Tuple2<Integer, List<Integer>>, Boolean> () {
							@Override
							public Boolean call(Tuple2<Integer, List<Integer>> v1) throws Exception {
								if(v1._2.get(v1._2.size()-1) > 0)
									return true;
								return false;
							}
						}).mapToPair(
						new PairFunction<Tuple2<Integer, List<Integer>>, String, Tuple2<Integer, Integer>>() {
							@Override
							public Tuple2<String, Tuple2<Integer, Integer>> call(
									Tuple2<Integer, List<Integer>> t) throws Exception {
								return new Tuple2( "" + t._2.get(1) + '_' + t._2.get(2), 
										new Tuple2(t._1,t._2.get(3)));
							}
						});
		return tmp.updateStateByKey(removeVehicleFromSegment, new HashPartitioner(LinearRoadMain.currentPartitions),
				globalSegmentState);
		
	}

	private static JavaPairDStream<String, Boolean> propagateAccidents(JavaStreamingContext ssc,
			JavaPairDStream<String, Tuple2<Tuple2<Long, List<Integer>>, SegmentState>> currentsegmentStream,
			JavaPairRDD<String, SegmentState> globalSegmentState) {
			JavaPairDStream<String, Boolean> accidentStream = currentsegmentStream.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String, Tuple2<Tuple2<Long, List<Integer>>, SegmentState>>, String, Boolean>() {
					@Override
					public Iterable<Tuple2<String, Boolean>> call(
							Tuple2<String, Tuple2<Tuple2<Long, List<Integer>>, SegmentState>> t) throws Exception {
						List<Tuple2<String, Boolean>> newList = new ArrayList<Tuple2<String, Boolean>>();
						String xway = t._1.split("_")[0];
						Integer segid = Integer.parseInt(t._1.split("_")[1]);
						Long time = System.currentTimeMillis() / 1000;
						// System.out.println("Generating upstream accidents for
						// key: "+t._1);
						if (t._2._2.hasAccident()) {
							// we generate true for upstream events
							for (int i = 0; i < 4; i++) {
								int newseg = segid - i;
								if (newseg < 0)
									continue;
								String key = xway + "_" + newseg;
								newList.add(new Tuple2(key, true));
							}
						} else if (t._2._2.minuteCleared > 0) {
							for (int i = 0; i < 4; i++) {
								int newseg = segid - i;
								if (newseg < 0)
									continue;
								String key = xway + "_" + newseg;
								newList.add(new Tuple2(key, false));
							}
						}
						return newList;
					}
				}).reduceByKey(new Function2<Boolean, Boolean, Boolean>() {
					@Override
					public Boolean call(Boolean v1, Boolean v2) throws Exception {
						// we eliminate duplicates
						return v1 || v2;
					}
				}, LinearRoadMain.currentPartitions);
		return accidentStream;
		/*				
		accidentStream.updateStateByKey(propagateAccidentEvent, 
				new HashPartitioner(LinearRoadMain.currentPartitions),
				globalSegmentState);*/
	}

	final static Function2<List<Tuple2<Integer, Integer>>, Optional<SegmentState>, Optional<SegmentState>> 
		removeVehicleFromSegment = new 
			Function2<List<Tuple2<Integer, Integer>>, Optional<SegmentState>, Optional<SegmentState>>() {
		@Override
		public Optional<SegmentState> call(List<Tuple2<Integer, Integer>> values, Optional<SegmentState> state) {
			SegmentState newState;
			if (state.isPresent()) {
				newState = state.get();
			} else {
				newState = new SegmentState();
				return Optional.of(newState);
			}
			// vehicle changed segment, we remove it from here
			for (Tuple2<Integer, Integer> t : values) {
				updateState(t, newState);
				// System.out.println("removeVehicleFromSegment:
				// "+v.toString());
			}

			return Optional.of(newState);
		}
	};
	
	private static SegmentState updateState(Tuple2<Integer, Integer> t , SegmentState s) {
		Integer vid = t._1;
		Integer pos = t._2;
		Vehicle v = s.containsVehicle(vid);
		if (v == null)
			return s;
		s.deleteVehicle(v);
		if (s.stoppedvehicles < 2) {
			s.resetAccident();
		}
		return s;
	}

	final static Function2<List<Boolean>, Optional<SegmentState>, Optional<SegmentState>> 
		propagateAccidentEvent = new Function2<List<Boolean>, Optional<SegmentState>, Optional<SegmentState>>() {
		@Override
		public Optional<SegmentState> call(List<Boolean> values, Optional<SegmentState> state) {
			SegmentState newState;
			if (state.isPresent()) {
				newState = state.get();
			} else {
				newState = new SegmentState();
			}
			if (newState.hasAccident())
				return Optional.of(newState);
			boolean accident = false;
			for (Boolean v : values)
				accident = accident || v;
			if (accident && !newState.hasAccident())
				newState.setUpstreamAccident(true);
			else if (newState.isUpstreamAccident() && !accident)
				newState.setUpstreamAccident(false);
			return Optional.of(newState);
		}
	};
	
	public static Optional<SegmentState> updateSegment(Tuple2<Long, List<Integer>> t, Optional<SegmentState> state) {
		SegmentState newState = state.get();
		// iterate over the tuples
			List<Integer> tuple = t._2;
			int vid = tuple.get(1);
			int time = tuple.get(0);
			if (time > newState.cTime)
				newState.cTime = time;
			Vehicle v = newState.containsVehicle(vid);
			if (v == null) {
				// new vehicle, must also report toll?
				v = new Vehicle(vid, t._1, tuple.get(3), tuple.get(4), tuple.get(6), tuple.get(7));
				newState.addVehicle(v);
				// System.out.print(v.toString()+"["+v.seg+"]"+" ");
			} else {
				// update info?
				if (tuple.get(3) == v.xway && tuple.get(4) == v.lane && tuple.get(7) == v.pos) {
					v.nsamepos++;
				} else {
					v.nsamepos = 0;
					if (v.stopped) {
						v.stopped = false;
						if (v.pastStopped)
							v.pastStopped = false;
						else
							v.pastStopped = true;
					}
				}
				if (v.nsamepos >= 4) {
					v.pastStopped = v.stopped;
					v.stopped = true;
					newState.stoppedVehicles.add(v.id);
				}
				/*
				 * System.out.println("Old v "+v.id+": "+v.xway+"_"+v.seg+
				 * " "+ v.pos+" "+v.lane+" "+v.dir+ " new: "+tuple.get(7)+
				 * " "+tuple.get(4)+" "+tuple.get(5));
				 */
				newState.updateVehicle(v, tuple.get(7), tuple.get(4), tuple.get(5), tuple.get(2), t._1);
			}
		
		// System.out.println("xx");
		List<Vehicle> to_remove = new ArrayList<Vehicle>();
		int nstopped = 0;
		if (newState.stoppedvehicles >= 2) {
			// check if there are at least 2 vehicles stopped at same pos
			boolean accident = false;
			for (int i = 0; i < newState.stoppedVehicles.size(); i++) {
				for (int j = i + 1; j < newState.stoppedVehicles.size(); j++) {
					if (((Vehicle)newState.currentVehicles.get(newState.stoppedVehicles.get(i))).pos ==
							((Vehicle)newState.currentVehicles.get(newState.stoppedVehicles.get(j))).pos
							&& ((Vehicle)newState.currentVehicles.get(newState.stoppedVehicles.get(i))).lane
							==((Vehicle)newState.currentVehicles.get(newState.stoppedVehicles.get(j))).lane)
						accident = true;
					break;
				}
			}
			if (accident) {
				newState.setAccident();
			} else if (newState.hasAccident())
				newState.resetAccident();
		}
		return Optional.of(newState);
	}

	final static Function2<List<Tuple2<SegmentState,Tuple2<Long, List<Integer>>>>, 
		Optional<SegmentState>, Optional<SegmentState>> 
		updateSegmentState = 
			new Function2<List<Tuple2<SegmentState,Tuple2<Long, List<Integer>>>>, 
			Optional<SegmentState>, Optional<SegmentState>>() {
		@Override 
		public Optional<SegmentState> call(List<Tuple2<SegmentState,Tuple2<Long, List<Integer>>>> values, 
				Optional<SegmentState> state) {
			SegmentState newState;
			if (state.isPresent()) {
				newState = state.get();
				newState.resetHasNewVehicles();
			} else {
				newState = new SegmentState();
			}
			for (Tuple2<SegmentState,Tuple2<Long, List<Integer>>> t : values) {
				updateSegment(t._2, Optional.of(newState));
			}
			return Optional.of(newState);
		}
	};

	/**
	 * 
	 * TOLL NOTIFICATION follows from here on
	 * 
	 */
	
	public static void outputTolls(
			JavaPairDStream<Integer, Tuple2<Long, List<Integer>>> positionStream,
			JavaPairDStream<String, Tuple2<Long, List<Integer>>> segmentStream,
			JavaPairDStream<String, SegmentState> stateDstream) {
		computeAndNotifyTolls(positionStream, segmentStream, stateDstream).
		foreachRDD(
				new Function2<JavaPairRDD<String, Tuple2<Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Double>, Integer>>, Time, Void>() {

					@Override
					public Void call(
							JavaPairRDD<String, Tuple2<Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Double>, Integer>> v1,
							Time v2) throws Exception {
						for (Tuple2<String, Tuple2<Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Double>, Integer>> elem : v1
								.collect()) {
							SegmentState s = elem._2._1._1._2;
							boolean hasAccident = s.hasAccident() || s.isUpstreamAccident();
							double avgspeed = elem._2._1._2;
							int nvehicles = elem._2._2;
							double toll = 0;
							if (hasAccident) {
								toll = 0;
							} else {
								if (nvehicles > 50 && avgspeed < 40)
									toll = 2 * (nvehicles - 50) * (nvehicles - 50);
							}
							for (Vehicle v : s.currentVehicles.values()) {
								if (v.isNew && v.lane != 4)
									System.out.println(
											"0" + "," + v.id + "," + v.time + "," + System.currentTimeMillis() / 1000
													+ "," + (int) avgspeed + "," + nvehicles + "," + (int) toll);
							}
						}
						return null;
					}
				});
	}
	
	private static JavaPairDStream<String, Tuple2<Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Double>, Integer>> 
		computeAndNotifyTolls(
			JavaPairDStream<Integer, Tuple2<Long, List<Integer>>> positionStream,
			JavaPairDStream<String, Tuple2<Long, List<Integer>>> segmentStream,
			JavaPairDStream<String, SegmentState> stateDstream) {
		// compute windowed avg speed per segmentStream
		JavaPairDStream<String, Double> avgbySegment = segmentStream.transformToPair(new
					Function2<JavaPairRDD<String, Tuple2<Long, List<Integer>>>, Time, 
					JavaPairRDD<String, Tuple2<Integer, Integer>>>() {
						@Override
						public JavaPairRDD<String, Tuple2<Integer, Integer>> call(
								JavaPairRDD<String, Tuple2<Long, List<Integer>>> v1, Time v2) throws Exception {
							return v1.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, 
									Tuple2<Long, List<Integer>>>>,
									String, Tuple2<Integer, Integer>>() {
										@Override
										public Iterable<Tuple2<String, Tuple2<Integer, Integer>>> call(
												Iterator<Tuple2<String, Tuple2<Long, List<Integer>>>> t)
														throws Exception {
											ArrayList<Tuple2<String, Tuple2<Integer, Integer>>> res = new 
													ArrayList<Tuple2<String, Tuple2<Integer, Integer>>>();
											while(t.hasNext()) {
												Tuple2<String, Tuple2<Long, List<Integer>>> v1 = t.next();
												String[] segmentl = v1._1.split("_");
												String segment = segmentl[0] + "_" + segmentl[1];
												res.add(new Tuple2(v1._1 + "_" + v1._2._2.get(1), new Tuple2(v1._2._2.get(2), 1)));
											}
											return res;
										}
							}, true);
						}
		}).reduceByKeyAndWindow(
						new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
							@Override
							public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1,
									Tuple2<Integer, Integer> v2) throws Exception {
								// System.out.println(""+v1._1+" "+v1._2+"
								// "+v2._1+" "+v2._2);
								return new Tuple2(v1._1 + v2._1, v1._2 + v2._2);
							}
						}, LinearRoadMain.windowAverage, LinearRoadMain.windowSlide,
						new SegmentPartitioner(LinearRoadMain.currentPartitions))
				.transformToPair(new
						Function2<JavaPairRDD<String, Tuple2<Integer, Integer>>, Time, 
						JavaPairRDD<String, Tuple2<Integer, Double>>>() {
							@Override
							public JavaPairRDD<String, Tuple2<Integer, Double>> call(
									JavaPairRDD<String, Tuple2<Integer, Integer>> v1, Time v2) throws Exception {
								return v1.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, 
										Tuple2<Integer, Integer>>>,
										String, Tuple2<Integer, Double>>() {
											@Override
											public Iterable<Tuple2<String, Tuple2<Integer, Double>>> call(
													Iterator<Tuple2<String, Tuple2<Integer, Integer>>> t)
															throws Exception {
												ArrayList<Tuple2<String, Tuple2<Integer, Double>>> res = new 
														ArrayList<Tuple2<String, Tuple2<Integer, Double>>>();
												while(t.hasNext()) {
													Tuple2<String, Tuple2<Integer, Integer>> v1 = t.next();
													String[] segmentl = v1._1.split("_");
													String segment = segmentl[0] + "_" + segmentl[1];
													res.add(new Tuple2(segment, new Tuple2(1, new Double(v1._2._1 / v1._2._2))));
												}
												return res;
											}
								}, true);
							}
					}).reduceByKey(
						new Function2<Tuple2<Integer, Double>, Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {
							@Override
							public Tuple2<Integer, Double> call(Tuple2<Integer, Double> v1, Tuple2<Integer, Double> v2)
									throws Exception {
								double speed = 0;
								int count = 0;
								count = v1._1 + v2._1;
								speed = v1._2 + v2._2;
								return new Tuple2(count, speed);
							}
						}, LinearRoadMain.currentPartitions)
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, Double>>, String, Double>() {
					@Override
					public Tuple2<String, Double> call(Tuple2<String, Tuple2<Integer, Double>> t) throws Exception {
						// System.out.println("xxx: "+t._1+" "+ t._2._1+"
						// "+t._2._2);
						return new Tuple2(t._1, new Double(t._2._2 / t._2._1));
					}
				});
		avgbySegment.cache();
		JavaPairDStream<String, Integer> mincarsbySegment = 
		segmentStream.transformToPair(new Function2<
						JavaPairRDD<String, Tuple2<Long, List<Integer>>>, Time, JavaPairRDD<String, Integer>>() {
							@Override
							public JavaPairRDD<String, Integer> call(
									JavaPairRDD<String, Tuple2<Long, List<Integer>>> v1, Time v2) throws Exception {
								return v1.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, Tuple2<Long, List<Integer>>>>, 
										String, Integer>() {
											@Override
											public Iterable<Tuple2<String, Integer>> call(
													Iterator<Tuple2<String, Tuple2<Long, List<Integer>>>> t)
															throws Exception {
												ArrayList<Tuple2<String, Integer>> resf = new ArrayList<Tuple2<String, Integer>>(); 
												while(t.hasNext()) {
													Tuple2<String, Tuple2<Long, List<Integer>>> v = t.next();
													resf.add(new Tuple2( v._1 + "_" + v._2._2.get(1), 1));
												}
												return resf;
											}
								}, true);
							}
				}).reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return 1;
					}
				}, LinearRoadMain.windowCount, LinearRoadMain.windowSlide,
						new SegmentPartitioner(LinearRoadMain.currentPartitions)).transformToPair(new
						Function2<JavaPairRDD<String, Integer>, Time, 
						JavaPairRDD<String, Integer>>() {
							@Override
							public JavaPairRDD<String, Integer> call(
									JavaPairRDD<String, Integer> v1, Time v2) throws Exception {
								return v1.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, 
										Integer>>, String, Integer>() {
											@Override
											public Iterable<Tuple2<String, Integer>> call(
													Iterator<Tuple2<String, Integer>> t)
															throws Exception {
												ArrayList<Tuple2<String, Integer>> res = new 
														ArrayList<Tuple2<String, Integer>>();
												while(t.hasNext()) {
													Tuple2<String, Integer> v1 = t.next();
													String[] segmentl = v1._1.split("_");
													String segment = segmentl[0] + "_" + segmentl[1];
													res.add(new Tuple2(segment, v1._2));
											}
												return res;
											}
								}, true);
							}
					}).reduceByKey(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
				}, LinearRoadMain.currentPartitions);

		JavaPairDStream<String, Tuple2<Tuple2<Long, List<Integer>>, SegmentState>> joinedstateDstream = segmentStream
				.join(stateDstream);
		JavaPairDStream<String, Tuple2<Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Double>, Integer>> avgperSegmentState = 
				joinedstateDstream.join(avgbySegment).join(mincarsbySegment);
		// we need to send notifications to vehicles which are new in this
		// stream
		JavaPairDStream<String, Tuple2<Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Double>, Integer>> newstateDstream = avgperSegmentState
				.filter(new Function<Tuple2<String, Tuple2<Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Double>, Integer>>, Boolean>() {
					@Override
					public Boolean call(
							Tuple2<String, Tuple2<Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Double>, Integer>> v1)
									throws Exception {
						SegmentState s = v1._2._1._1._2;
						return s.hasNewVehicles();
					}
				});
		return newstateDstream;
	}
}
