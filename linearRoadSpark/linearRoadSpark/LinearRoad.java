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
import scala.Tuple4;

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


	public static JavaPairDStream<Integer, Tuple2<Long, List<Integer>>> parseInitialTuples(JavaStreamingContext ssc, 
			String host, String port) {
		JavaReceiverInputDStream<String> lines = ssc.socketTextStream(host, Integer.parseInt(port), 
				StorageLevels.MEMORY_AND_DISK);
		return lines.transformToPair(new Function<JavaRDD<String>,
				JavaPairRDD<Integer, Tuple2<Long, List<Integer>>> >() {
					@Override
					public JavaPairRDD<Integer, Tuple2<Long, List<Integer>>> call(JavaRDD<String> v1) throws Exception {
					JavaPairRDD<Integer, Tuple2<Long, List<Integer>>> res = v1.mapPartitionsToPair(
							new PairFlatMapFunction<Iterator<String>, Integer, Tuple2<Long, List<Integer>>>() {
					@Override
					public Iterable<Tuple2<Integer, Tuple2<Long, List<Integer>>>> call(Iterator<String> x) {
						// return a tuple identified by key
						// Type = 0, Time, VID, Spd, XWay, Lane, Dir, Seg, Pos
						List<Tuple2<Integer, Tuple2<Long, List<Integer>>>> res = new ArrayList<Tuple2<Integer, Tuple2<Long, List<Integer>>>>();
						Iterator<String> it = x;
						while(it.hasNext()) {
							String elem = it.next();
							ArrayList elems = Lists.newArrayList(SPACE.split(elem.trim()));
							
							Integer key = Integer.parseInt((String) elems.get(0));
							elems.remove(0);
							Long time = Long.parseLong((String)elems.get(0));
							elems.remove(0);
							ArrayList<Integer> ints = new ArrayList<Integer>();
							ints.add(0);
							for (Object e : elems)
								ints.add(Integer.parseInt((String) e));
							res.add( new Tuple2(key, new Tuple2(time, ints)));
						}
						return res;
					}
				});
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
		return segmentStream.leftOuterJoin(data).updateStateByKey(updateSegmentState, 
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
	
	
	
	static void printAccidents(JavaPairDStream<String, Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>,Boolean>> s) {
		 s.foreachRDD(new
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

	

	public static JavaPairDStream<String, Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Boolean>> computeAccidents(
			JavaStreamingContext ssc, JavaPairDStream<String, Tuple2<Long, List<Integer>>> segmentStream,
			JavaPairDStream<Integer, Tuple2<Long, List<Integer>>> positionStream,
			JavaPairDStream<String, SegmentState> stateDstream) {
		
		JavaPairDStream<String, Tuple2<Tuple2<Long, List<Integer>>, SegmentState>> currentsegmentStream = segmentStream
				.join(stateDstream);
		JavaPairDStream<String, Boolean> propagatedAccidentStream = propagateAccidents(ssc, currentsegmentStream,
				globalSegmentState);
		return currentsegmentStream.join(propagatedAccidentStream);
	}

	public static JavaPairDStream<String, Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Boolean>> getAccidentSegments(
			JavaPairDStream<String, Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Boolean>> propagatedAccidentStream) {
		return propagatedAccidentStream
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
				.mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Long, List<Integer>>>, Integer, Tuple2<Long,List<Integer>>>() {
					@Override
					public Tuple2<Integer, Tuple2<Long,List<Integer>>> call(Tuple2<Integer, Tuple2<Long, List<Integer>>> s) {
						ArrayList<Integer> elems = new ArrayList();
						elems.add(s._2._2.get(3)); // xway (0)
						elems.add(s._2._2.get(6)); // segment (1)
						elems.add(s._2._2.get(7)); // position (2)
						elems.add(0); // does vehicle changed segment or not? for reduce
						// with transform to keep the key and then at reduce to use the segmentpartitioner
						return new Tuple2(s._2._2.get(1), new Tuple2(s._2._1, elems));
					}
				})
				.reduceByKeyAndWindow(
						new Function2<Tuple2<Long,List<Integer>>, Tuple2<Long,List<Integer>>, Tuple2<Long,List<Integer>>>() {
							@Override
							public Tuple2<Long,List<Integer>> call(Tuple2<Long,List<Integer>> v1,
									Tuple2<Long,List<Integer>> v2) throws Exception {
								if (v1._2.get(2) == v2._2.get(2)) {
									if(v1._1 > v2._1)
										return v1;
									else
										return v2;
								}
								if(v1._1 > v2._1) {
									v2._2.remove(v2._2.size()-1);
									v2._2.add(v2._2.size()-1, 1); // we mark the change in segment
									return v2;
								}
								v1._2.remove(v1._2.size()-1);
								v1._2.add(v1._2.size()-1, 1); // we mark the change in segment
								return v1;
							}
							
						}, new Duration(60000), LinearRoadMain.windowSlide).
							filter(new Function<Tuple2<Integer, Tuple2<Long,List<Integer>>>, Boolean> () {
							@Override
							public Boolean call(Tuple2<Integer, Tuple2<Long,List<Integer>>> v1) throws Exception {
								if(v1._2._2.get(v1._2._2.size()-1) > 0)
									return true;
								return false;
							}
						}).mapToPair(
						new PairFunction<Tuple2<Integer, Tuple2<Long,List<Integer>>>, String, Tuple2<Integer, Integer>>() {
							@Override
							public Tuple2<String, Tuple2<Integer, Integer>> call(
									Tuple2<Integer, Tuple2<Long,List<Integer>>> t) throws Exception {
								// <xway_segment, <vid, position>>
								Tuple2<String, Tuple2<Integer, Integer>> res = new Tuple2("1_1", new Tuple2(0,0));
								try {
									res = new Tuple2( "" + t._2._2.get(0) + '_' + t._2._2.get(1), 
											new Tuple2(t._1,t._2._2.get(2)));
								} catch (Exception e){
									System.out.println(t.toString());
									throw e;
								}
								finally {
									
								}
								return res;
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
						System.out.println("Generating upstream accidents for key: "+t._1);
						if (t._2._2.hasAccident()) {
							// we generate true for upstream events
							for (int i = 0; i < 4; i++) {
								int newseg = segid - i;
								if (newseg < 0)
									continue;
								String key = xway + "_" + newseg;
								newList.add(new Tuple2(key, true));
							}
						} else {
							/*if (t._2._2.minuteCleared > 0) {
							for (int i = 0; i < 4; i++) {
								int newseg = segid - i;
								if (newseg < 0)
									continue;
								String key = xway + "_" + newseg;
								newList.add(new Tuple2(key, false));
							}*/
							String key = xway + "_" + segid;
							newList.add(new Tuple2(key, false));
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

	final static Function2<List<Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>,Boolean>>, Optional<SegmentState>, Optional<SegmentState>> 
		propagateAccidentEvent = new Function2<List<Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>,Boolean>>, 
									Optional<SegmentState>, Optional<SegmentState>>() {
		@Override
		public Optional<SegmentState> call(List<Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>,Boolean>> values, 
				Optional<SegmentState> state) {
			SegmentState newState;
			if (state.isPresent()) {
				newState = state.get();
			} else {
				newState = new SegmentState();
			}
			if (newState.hasAccident())
				return Optional.of(newState);
			boolean accident = false;
			for (Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>,Boolean> v : values)
				accident = accident || v._2;
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
			long time = t._1;
			if (time > newState.cTime)
				newState.cTime = time;
			Vehicle v = newState.containsVehicle(vid);
			if (v == null) {
				// new vehicle, must also report toll?
				v = new Vehicle(vid, t._1, tuple.get(3), tuple.get(4), tuple.get(6), tuple.get(7));
				newState.addVehicle(v);
				System.out.print(v.toString()+"["+v.seg+"]"+" ");
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

	final static Function2<List<Tuple2<Tuple2<Long, List<Integer>>, Optional<SegmentState> > >, 
		Optional<SegmentState>, Optional<SegmentState>> 
		updateSegmentState = 
			new Function2<List<Tuple2<Tuple2<Long, List<Integer>>, Optional<SegmentState> > >, 
			Optional<SegmentState>, Optional<SegmentState>>() {
		@Override 
		public Optional<SegmentState> call(List<Tuple2<Tuple2<Long, List<Integer>>, Optional<SegmentState> > > values, 
				Optional<SegmentState> state) {
			SegmentState newState;
			if (state.isPresent()) {
				newState = state.get();
				newState.resetHasNewVehicles();
			} else {
				newState = new SegmentState();
			}
			for (Tuple2<Tuple2<Long, List<Integer>>, Optional<SegmentState> > t : values) {
				updateSegment(t._1, Optional.of(newState));
			}
			return Optional.of(newState);
		}
	};

	/**
	 * 
	 * TOLL NOTIFICATION follows from here on
	 * 
	 */
	
	public static void printTolls(
			JavaPairDStream<String, Tuple2<Tuple2< Tuple2<Tuple2<Tuple2<Long, List<Integer>>, 
				SegmentState>, Boolean>, Integer>, Integer>> s) {
			s.foreachRDD(
				new Function2<JavaPairRDD<String, Tuple2<Tuple2< Tuple2<Tuple2<Tuple2<Long, List<Integer>>, 
				SegmentState>, Boolean>, Integer>, Integer>>, Time, Void>() {
					@Override
					public Void call(
							JavaPairRDD<String, Tuple2<Tuple2< Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Boolean>, Integer>, Integer>> v1,
							Time v2) throws Exception {
						for (Tuple2<String, Tuple2<Tuple2< Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Boolean>, Integer>, Integer>> elem : v1
								.collect()) {
							SegmentState s = elem._2._1._1._1._2;
							boolean hasAccident = elem._2._1._1._2;
							int avgspeed = elem._2._1._2;
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
	
	public static void printTolls1(
			JavaPairDStream<String, Tuple2<Tuple2<Long, List<Integer>>, Tuple2<Integer,Integer>>> s) {
			s.foreachRDD(
				new Function2<JavaPairRDD<String, Tuple2<Tuple2<Long, List<Integer>>, Tuple2<Integer,Integer>>>, Time, Void>() {
					@Override
					public Void call(
							JavaPairRDD<String,  Tuple2<Tuple2<Long, List<Integer>>, Tuple2<Integer,Integer>>> v1,
							Time v2) throws Exception {
						for (Tuple2<String,  Tuple2<Tuple2<Long, List<Integer>>, Tuple2<Integer,Integer>>> elem : v1.collect()) {
							int avgspeed = elem._2._2._2;
							int nvehicles = elem._2._2._1;
							double toll = 0;
							if (nvehicles > 50 && avgspeed < 40)
									toll = 2 * (nvehicles - 50) * (nvehicles - 50);
									System.out.println(
											"0" + "," + elem._2._1._2.get(1) + "," + elem._2._1._1 + "," + System.currentTimeMillis() / 1000
													+ "," + (int) avgspeed + "," + nvehicles + "," + (int) toll);
					}
					   return null;
					}
				});
	}
	
	public static JavaPairDStream<String, Tuple2<Tuple2<Long, List<Integer>>, Tuple2<Integer,Integer>>> computeAndNotifyTolls(
			JavaPairDStream<Integer, Tuple2<Long, List<Integer>>> positionStream,
			JavaPairDStream<String, Tuple2<Long, List<Integer>>> segmentStream,
			JavaPairDStream<String, Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Boolean>> stateDstream) {
		// compute windowed avg speed per segmentStream
		JavaPairDStream<String, Tuple4<Long, Integer, Integer,Integer>> avgbyVidMinute = segmentStream.transformToPair(new
					Function2<JavaPairRDD<String, Tuple2<Long, List<Integer>>>, Time, 
					JavaPairRDD<String, Tuple4<Long, Integer, Integer, Integer>>>() {
						@Override
						public JavaPairRDD<String, Tuple4<Long, Integer, Integer, Integer>> call(
								JavaPairRDD<String, Tuple2<Long, List<Integer>>> v1, Time v2) throws Exception {
							return v1.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, 
									Tuple2<Long, List<Integer>>>>,
									String, Tuple4<Long, Integer, Integer, Integer>>() {
										@Override
										public Iterable<Tuple2<String, Tuple4<Long, Integer, Integer, Integer>>> call(
												Iterator<Tuple2<String, Tuple2<Long, List<Integer>>>> t)
														throws Exception {
											ArrayList<Tuple2<String, Tuple4<Long, Integer, Integer, Integer>>> res = new 
													ArrayList<Tuple2<String, Tuple4<Long, Integer, Integer, Integer>>>();
											while(t.hasNext()) {
												Tuple2<String, Tuple2<Long, List<Integer>>> v1 = t.next();
												String[] segmentl = v1._1.split("_");
												String segment = segmentl[0] + "_" + segmentl[1];
												res.add(new Tuple2(v1._1 + "_" + v1._2._2.get(1), 
														new Tuple4(v1._2._1, 1, v1._2._2.get(2), 1)));
											}
											return res;
										}
							}, true);
						}
		}).reduceByKeyAndWindow(
						new Function2<Tuple4<Long, Integer, Integer, Integer>, 
							Tuple4<Long, Integer, Integer, Integer>, 
							Tuple4<Long, Integer, Integer, Integer>>() {
							@Override
							public Tuple4<Long, Integer, Integer, Integer> call(Tuple4<Long, Integer, Integer, Integer> v1,
									Tuple4<Long, Integer, Integer, Integer> v2) throws Exception {
								// System.out.println(""+v1._1+" "+v1._2+"
								// "+v2._1+" "+v2._2);
								// speed sum, count
								Long time = v1._1() > v2._1() ? v1._1() : v2._1();
								return new Tuple4(time, v1._2() + v2._2(), v1._3() + v2._3(), v1._4() + v2._4());
							}
						}, LinearRoadMain.windowCount, LinearRoadMain.windowSlide,
						new SegmentPartitioner(LinearRoadMain.currentPartitions));
		avgbyVidMinute.cache();
		
		JavaPairDStream<String, Tuple2<Integer,Integer>> avgbySegment = avgbyVidMinute.reduceByKeyAndWindow(
						new Function2<Tuple4<Long, Integer, Integer, Integer>, Tuple4<Long, Integer, Integer, Integer>, 
								Tuple4<Long, Integer, Integer, Integer>>() {
							@Override
							public Tuple4<Long, Integer, Integer, Integer> call(Tuple4<Long, Integer, Integer, Integer> v1, 
									Tuple4<Long, Integer, Integer, Integer> v2)
									throws Exception {
								int speed = 0;
								int count = 0;
								speed = v1._2() + v2._2();
								count = v1._3() + v2._3();
								
								Long time = new Long(0);
		    					Integer value =0;
		    					if (v1._1() > v2._1()) {
		    						time = v1._1();
		    						value = v1._2();
		    					} else {
		    						time = v2._1();
		    						value = v2._2();
		    					}
		    					return new Tuple4(time, value, v1._3() + v2._3(), v1._4() + v2._4());
							}
						}, LinearRoadMain.windowAverage, LinearRoadMain.windowSlide,
			new SegmentPartitioner(LinearRoadMain.currentPartitions)).
				transformToPair(new
					Function2<JavaPairRDD<String, Tuple4<Long, Integer, Integer, Integer>>, Time, 
					JavaPairRDD<String, Tuple4<Long, Integer, Integer, Integer>>>() {
						@Override
						public JavaPairRDD<String, Tuple4<Long, Integer, Integer, Integer>> call(
								JavaPairRDD<String, Tuple4<Long, Integer, Integer, Integer>> v1, Time v2) throws Exception {
							return v1.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, 
									Tuple4<Long, Integer, Integer, Integer>>>,
									String, Tuple4<Long, Integer, Integer, Integer>>() {
										@Override
										public Iterable<Tuple2<String, Tuple4<Long, Integer, Integer, Integer>>> call(
												Iterator<Tuple2<String, Tuple4<Long, Integer, Integer, Integer>>> t)
														throws Exception {
											ArrayList<Tuple2<String, Tuple4<Long, Integer, Integer, Integer>>> res = new 
													ArrayList<Tuple2<String, Tuple4<Long, Integer, Integer, Integer>>>();
											while(t.hasNext()) {
												Tuple2<String, Tuple4<Long, Integer, Integer, Integer>> v1 = t.next();
												String[] segmentl = v1._1.split("_");
												String segment = segmentl[0] + "_" + segmentl[1];
												res.add(new Tuple2<String, Tuple4<Long, Integer, Integer, Integer>>
													(segment, new Tuple4<Long, Integer, Integer, Integer>(v1._2._1(), 1, 1, 
														v1._2._4() / v1._2._3())));
											}
											return res;
										}
							}, true);
						}
				}).reduceByKey(
						new Function2<Tuple4<Long, Integer, Integer, Integer>, Tuple4<Long, Integer, Integer, Integer>, Tuple4<Long, Integer, Integer, Integer>>() {
							@Override
							public Tuple4<Long, Integer, Integer, Integer> call(Tuple4<Long, Integer, Integer, Integer> v1, 
									Tuple4<Long, Integer, Integer, Integer> v2)
									throws Exception {
								
								Long time1 = v1._1()/60;
		    					Long time2 = v2._1()/60;
		    					Long time = time2;
		    					Integer value = new Integer(0);
		    					if(time1 < time2) {
		    						value = v2._2();
		    					} else if (time1>time2) {
		    						value = v2._2();
		    						time = time1;
		    					}
		    					value = v2._2() + v1._2();
								return new Tuple4(time, value, v1._3() + v2._3(), v2._4() + v2._4());
							}
						}, LinearRoadMain.currentPartitions)
				.mapToPair(new PairFunction<Tuple2<String, Tuple4<Long, Integer, Integer, Integer>>, String,  Tuple2<Integer, Integer>>() {
					@Override
					public Tuple2<String, Tuple2<Integer,Integer>> call(Tuple2<String, Tuple4<Long,Integer, Integer, Integer>> t) throws Exception {
						// System.out.println("xxx: "+t._1+" "+ t._2._1+"
						// "+t._2._2);
						return new Tuple2<String, Tuple2<Integer, Integer>>(t._1, new Tuple2<Integer, Integer>(t._2._2(), t._2._4() / t._2._3()));
					}
				});
		avgbySegment.cache();
		
				
		JavaPairDStream<String, Tuple2<Tuple2<Long, List<Integer>>, Tuple2<Integer,Integer>>> avgperSegmentState = 
				segmentStream.join(avgbySegment);
		
		//avgbySegment.print();
		//mincarsbySegment.print();
		//avgperSegmentState.print();
		
		// we need to send notifications to vehicles which are new in this
		// stream
		/*JavaPairDStream<String, Tuple2<Tuple2< Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Boolean>, Integer>, Integer>> newstateDstream = avgperSegmentState
				.filter(new Function<Tuple2<String, Tuple2<Tuple2< Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Boolean>, Integer>, Integer>>, Boolean>() {
					@Override
					public Boolean call(
							Tuple2<String, Tuple2<Tuple2<Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Boolean>, Integer>, Integer>> v1)
									throws Exception {
						SegmentState s = v1._2._1._1._1._2;
						return s.hasNewVehicles();
					}
				});*/
		return avgperSegmentState;
	}
}
