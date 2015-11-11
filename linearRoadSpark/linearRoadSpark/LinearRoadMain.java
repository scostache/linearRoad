package linearRoadSpark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
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

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple6;
import scala.Tuple7;

public class LinearRoadMain {
	private static final Pattern SPACE = Pattern.compile(",");
	protected static int batchInteval = 3000;
	protected static final Duration windowAverage = new Duration(300000); // 5 minutes
	protected static final Duration windowSlide = new Duration(batchInteval); // 3 seconds slide
	protected static final Duration windowCount = new Duration(60000);
	
	protected static int currentPartitions;
	protected static final int maxCheckpoint = 4;
	
	private static int checkpointCounter = 0;
	private final static int maxCheckpointCounter = 10;
	
	private static List<Tuple2<String, AccidentSegmentState>> acctuples = Arrays.asList(
			new Tuple2<String, AccidentSegmentState>("1_0", new AccidentSegmentState()),
			new Tuple2<String, AccidentSegmentState>("1_1", new AccidentSegmentState()));
	private static JavaPairRDD<String, AccidentSegmentState> accidentState = null; 
	private static List<Tuple2<String, TollSegmentState>> tolltuples = Arrays.asList(
			new Tuple2<String, TollSegmentState>("1_0", new TollSegmentState()),
			new Tuple2<String, TollSegmentState>("1_1", new TollSegmentState()));
	private static JavaPairRDD<String, TollSegmentState> tollState = null;
	
	// TODO: we need to filter the vehicles which changed segment....
	
	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage: LinearRoad <hostname> <port>");
			System.exit(1);
		}
		if(args.length >=3) {
			currentPartitions = Integer.parseInt(args[2]);
		} else {
			currentPartitions = 2;
		}
		if(args.length == 4) {
			batchInteval = Integer.parseInt(args[3]);
		}
		
		System.out.println(args[0] + " " + args[1]);
		// Create the context with a 1 second batch size
		SparkConf sparkConf = new SparkConf().setAppName("LinearRoad");
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sparkConf.set("spark.default.parallelism", "" + LinearRoadMain.currentPartitions);
		sparkConf.set("spark.streaming.blockInterval", "200");
		//sparkConf.set("spark.streaming.unpersist", "false");
		sparkConf.set("spark.speculation", "true");
		sparkConf.set("spark.executor.memory", "800m");
		Class[] array = { SegmentState.class, Vehicle.class };
		sparkConf.registerKryoClasses(array);
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(batchInteval));
		System.out.println("Min number of partitions: " + ssc.sc().defaultMinPartitions() + " default parallelism: "
				+ ssc.sc().defaultParallelism());
		
		ssc.checkpoint("hdfs://spark1:54310/checkpoint/");
		
		accidentState = ssc.sc().parallelizePairs(acctuples, LinearRoadMain.currentPartitions);
		tollState = ssc.sc().parallelizePairs(tolltuples, LinearRoadMain.currentPartitions);
		
		JavaPairDStream<Integer, Tuple2<Long, List<Integer>>> initTuples = parseInitialTuples(ssc, args[0], args[1]); // we group by type
		JavaPairDStream<Integer, Tuple2<Long, List<Integer>>> positionStream = generatePositionStream(initTuples); // we filter and output only position tuples
		JavaPairDStream<String, Tuple2<Long, List<Integer>>> segmentStream = generateSegmentStream(positionStream); // we group by segment_id
		
		JavaPairDStream<String, Tuple2<Boolean, Long>> accidentStream = segmentStream.groupByKey(new HashPartitioner(LinearRoadMain.currentPartitions)).transformToPair(
				new Function2<JavaPairRDD<String, Iterable<Tuple2<Long, List<Integer>>>>, Time, JavaPairRDD<String, Tuple2<Boolean, Long>>>() {
					private static final long serialVersionUID = -9202502146928153066L;
					@Override
					public JavaPairRDD<String, Tuple2<Boolean, Long>> call(
							JavaPairRDD<String, Iterable<Tuple2<Long, List<Integer>>>> v1, Time v2) throws Exception {
						JavaPairRDD<String, Tuple2<Iterable<Tuple2<Long, List<Integer>>>, Optional<AccidentSegmentState>>> tmpRdd = v1.leftOuterJoin(accidentState);
						accidentState.unpersist(false);
						// we update the accident state here
						accidentState = tmpRdd.mapToPair(new DetectAccidents()).persist(StorageLevel.MEMORY_AND_DISK());
						if (checkpointCounter > maxCheckpointCounter) {
					        accidentState.checkpoint();
					        checkpointCounter=0;
					      }
					      else checkpointCounter=checkpointCounter+1;
						// the first one was to update the state, now we merge with the new state and we output the accident segments
						return v1.join(accidentState).flatMapToPair(new OutputAccidentSegments());
					}
				});
		JavaDStream<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>> alltuplesTollAccident =
					segmentStream.join(accidentStream).transform(
							new Function2<JavaPairRDD<String, Tuple2<Tuple2<Long, List<Integer>>, Tuple2<Boolean, Long>>>, Time, 
								JavaRDD<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>>>() {
								
								private static final long serialVersionUID = -9202502146928153066L;
								@Override
								public JavaRDD<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>> call(
										JavaPairRDD<String, Tuple2<Tuple2<Long, List<Integer>>, Tuple2<Boolean, Long>>> v1, Time v2) throws Exception {
											JavaPairRDD<String, Tuple2<Tuple2<Tuple2<Long, List<Integer>>, Tuple2<Boolean, Long>>, Optional<TollSegmentState>>> tmpRdd = 
												v1.leftOuterJoin(tollState);
									tollState.unpersist(false);
									// we update the accident state here
									tollState = tmpRdd.mapToPair(new ComputeTolls()).persist(StorageLevel.MEMORY_AND_DISK());
									if (checkpointCounter == 0) {
								        tollState.checkpoint();
								     }
									// the first one was to update the state, now we merge with the new state and we output the accident segments
									return v1.join(tollState).flatMap(new OutputTollsAndAccidents());
								}
							});
		
		/*
		 * filtering and output
		 */
		JavaDStream<Tuple6<Integer, Integer, Long, Long, Integer, Integer>> accidentTuples =
				alltuplesTollAccident.filter(
						new Function<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>, Boolean>() {
							@Override
							public Boolean call(Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double> value)
									throws Exception {
								if(value._1() == 1)
									return true;
								return false;
							}
						}).map(new Function<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>,
								Tuple6<Integer, Integer, Long, Long, Integer, Integer>>() {
									@Override
									public Tuple6<Integer, Integer, Long, Long, Integer, Integer> call(
											Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double> value)
													throws Exception {
										return new Tuple6<Integer, Integer, Long, Long, Integer, Integer>(
												value._1(), value._2(), value._3(), value._4(), value._5(), value._6());
									}
						});
		JavaDStream<Tuple6<Integer, Integer, Long, Long, Integer, Integer>> tollTuples = 
				alltuplesTollAccident.filter(new Function<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>, Boolean>() {
					@Override
					public Boolean call(Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double> value)
							throws Exception {
						if(value._1() == 0)
							return true;
						return false;
					}
				}).map(new Function<Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>,
						Tuple6<Integer, Integer, Long, Long, Integer, Integer>>() {
					@Override
					public Tuple6<Integer, Integer, Long, Long, Integer, Integer> call(
							Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double> value)
									throws Exception {
						return new Tuple6<Integer, Integer, Long, Long, Integer, Integer>(
								value._1(), value._2(), value._3(), value._4(), value._5(), value._6().intValue());
					}
		});
		
		accidentTuples.print();
		tollTuples.print();
		
		ssc.start();
		ssc.awaitTermination();
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
	
	private static class OutputAccidentSegments implements PairFlatMapFunction<Tuple2<String,Tuple2<Iterable<Tuple2<Long, List<Integer>>>, 
		AccidentSegmentState>>, String,
	Tuple2<Boolean, Long>> {

		@Override
		public Iterable<Tuple2<String, Tuple2<Boolean, Long>>> call(
				Tuple2<String, Tuple2<Iterable<Tuple2<Long, List<Integer>>>, AccidentSegmentState>> t)
						throws Exception {
			ArrayList<Tuple2<String, Tuple2<Boolean, Long>>> collector = new ArrayList<Tuple2<String, Tuple2<Boolean, Long>>>();
			boolean newAcc = t._2()._2().isNewAccident();
			boolean newClear = t._2()._2().isCleared();
			if ( newAcc) {
				long time = t._2()._2().getTimeNew();
				int segment = Integer.parseInt(t._1.split("_")[1]);
				int xway = Integer.parseInt(t._1.split("_")[0]);
				for (int i = 0; i <= 4; i++) {
					int realseg = segment - i;
					if (realseg < 0)
						break;
					collector.add(new Tuple2<String, Tuple2<Boolean, Long>>(xway + "_" + realseg, new Tuple2(true, time)));
				}
			} else {
				if (newClear) {
					int segment = Integer.parseInt(t._1.split("_")[1]);
					int xway = Integer.parseInt(t._1.split("_")[0]);
					long time = t._2()._2().getTimeCleared();
					collector.add(new Tuple2<String, Tuple2<Boolean, Long>>(xway + "_" + segment, new Tuple2(false, time)));
					for (int i = 1; i <= 4; i++) {
						int realseg = segment - i;
						if (realseg < 0)
							break;
						collector.add(new Tuple2<String, Tuple2<Boolean, Long>>(xway + "_" + realseg, new Tuple2(false, time)));
					}
				}
			}
			return collector;
		}
		
	}
	
	private static class DetectAccidents implements PairFunction<Tuple2<String,Tuple2<Iterable<Tuple2<Long, List<Integer>>>, 
		Optional<AccidentSegmentState>>>, String,
									AccidentSegmentState> {
		private static final long serialVersionUID = 11625252L;

		@Override
		public Tuple2<String, AccidentSegmentState> call(Tuple2<String,Tuple2<Iterable<Tuple2<Long, List<Integer>>>, 
				Optional<AccidentSegmentState>>> v) throws Exception {
			AccidentSegmentState newState; // the segment state for this tuple
			if(v._2._2.isPresent()) {
				newState = v._2._2.get();
			} else {
				newState = new AccidentSegmentState();
			}
			
			for (Tuple2<Long, List<Integer>> t : v._2._1) {
				// we update the state ?
				//VID, Spd, XWay, Lane, Dir, Seg, Pos
				newState.addVehicle(t._2.get(0), t._1, t._2.get(2), t._2.get(3), t._2.get(5), t._2.get(6), t._2.get(1));
				newState.updateSegmentState(t._2.get(0), t._1, t._2.get(2), t._2.get(3), t._2.get(5), t._2.get(6), t._2.get(1));
			}
			return new Tuple2<String, AccidentSegmentState>(v._1, newState);
		}

	}
	
	private static class ComputeTolls implements PairFunction<Tuple2<String, Tuple2<Tuple2<Tuple2<Long, List<Integer>>, 
		Tuple2<Boolean, Long>>, Optional<TollSegmentState>>>, 
	String, TollSegmentState> {
		private static final long serialVersionUID = 199484848L;

		@Override
		public Tuple2<String, TollSegmentState> call(Tuple2<String, Tuple2<Tuple2<Tuple2<Long, List<Integer>>, Tuple2<Boolean, Long>>, 
				Optional<TollSegmentState>>> value)
				throws Exception {
			TollSegmentState newState;
			if (value._2._2.isPresent()) {
				newState = value._2._2.get();
			} else {
				newState = new TollSegmentState();
			}
			Tuple2<Long, List<Integer>> t = value._2()._1()._1();
			//VID, Spd, XWay, Lane, Dir, Seg, Pos
			newState.computeTolls(t._1(), t._2.get(0), t._2.get(5), t._2.get(3), t._2.get(6), t._2.get(1));
			// update accident info
			newState.markAndClearAccidents(value._2._1._2());
			
			return new Tuple2<String, TollSegmentState>(value._1, newState);
		}
	}
	
	private static class OutputTollsAndAccidents implements FlatMapFunction<Tuple2<String, Tuple2<Tuple2<Tuple2<Long, List<Integer>>, 
			Tuple2<Boolean, Long>>, TollSegmentState>>, Tuple7<Integer, Integer, Long, Long, Integer, Integer,Double>> {
		@Override
		public Iterable<Tuple7<Integer, Integer, Long, Long, Integer, Integer,Double>> call(
				Tuple2<String, Tuple2<Tuple2<Tuple2<Long, List<Integer>>, Tuple2<Boolean, Long>>, TollSegmentState>> t)
						throws Exception {
			ArrayList<Tuple7<Integer, Integer, Long, Long, Integer, Integer,Double>> collector = 
					new ArrayList<Tuple7<Integer, Integer, Long, Long, Integer, Integer,Double>>();
			TollSegmentState cstate = t._2()._2();
			long time = t._2()._1()._1()._1();
			int lane = t._2()._1()._1()._2().get(3);
			int vid = t._2()._1()._1()._2().get(0);
			int segment = t._2()._1()._1()._2().get(5);
			int position = t._2()._1()._1()._2().get(6);
			String segid = t._1();
			boolean outputAcc = cstate.needToOutputAccident(time, lane);
			if(outputAcc) {
				collector.add(new Tuple7<Integer, Integer, Long, Long, Integer, Integer,Double>
					(1, vid, time, System.currentTimeMillis(), segment, position,0.0));
			}
			double vtoll = cstate.getCurrentToll(t._1(), outputAcc);
			int sspeed = (int) cstate.getLav();
			collector.add(new Tuple7<Integer, Integer, Long, Long, Integer, Integer,Double>
				(0, vid, time, System.currentTimeMillis(), segment, sspeed, vtoll));
			return collector;
		}
	}
	
}
