package linearRoadSpark;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
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
	private static final Pattern SPACE = Pattern.compile(" ");
	protected static int batchInteval = 1000;
	protected static final Duration windowAverage = new Duration(300000); // 5
																			// minutes
	protected static final Duration windowSlide = new Duration(batchInteval); // 3
																				// seconds
																				// slide
	protected static final Duration windowCount = new Duration(60000);

	private static int currentPartitions;
	private static final int vidPartitions = 2;

	private static int checkpointCounter = 0;
	private final static int maxCheckpointCounter = 10;
	private final static int maxCheckpointCounterVid = 10;

	private static List<Tuple2<String, AccidentSegmentState>> acctuples = Arrays.asList(
			new Tuple2<String, AccidentSegmentState>("1_0", new AccidentSegmentState()),
			new Tuple2<String, AccidentSegmentState>("1_1", new AccidentSegmentState()));
	private static JavaPairRDD<String, AccidentSegmentState> accidentState = null;
	private static List<Tuple2<String, TollSegmentState>> tolltuples = Arrays.asList(
			new Tuple2<String, TollSegmentState>("1_0", new TollSegmentState()),
			new Tuple2<String, TollSegmentState>("1_1", new TollSegmentState()));
	private static JavaPairRDD<String, TollSegmentState> tollState = null;

	private static List<Tuple2<Integer, String>> vehiclePreviousSegments = Arrays
			.asList(new Tuple2<Integer, String>(-1, "1_0"), new Tuple2<Integer, String>(-2, "1_1"));
	private static JavaPairRDD<Integer, String> vehiclePreviousSegmentState = null;

	private static int checkpointCounterVid = 0;

	// TODO: we need to filter the vehicles which changed segment....

	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: LinearRoad <hostname_file>");
			System.exit(1);
		}
		if (args.length >= 2) {
			currentPartitions = Integer.parseInt(args[1]);
		} else {
			currentPartitions = 2;
		}
		if (args.length == 3) {
			batchInteval = Integer.parseInt(args[2]);
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
		
		// Create the context with a 1 second batch size
		SparkConf sparkConf = new SparkConf().setAppName("LinearRoad");
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sparkConf.set("spark.default.parallelism", "" + LinearRoadMain.currentPartitions);
		sparkConf.set("spark.streaming.blockInterval", "200");
		// sparkConf.set("spark.streaming.unpersist", "false");
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
		vehiclePreviousSegmentState = ssc.sc().parallelizePairs(vehiclePreviousSegments, 2);
		ArrayList<JavaPairDStream<String, LRTuple>> arr = new ArrayList<JavaPairDStream<String,LRTuple>>();
		for (Tuple2<String, Integer> t : recHosts) {
			JavaPairDStream<String, LRTuple> initTuples = parseInitialTuples(ssc, t._1, t._2);
			arr.add(initTuples);
		}
		JavaPairDStream<String,LRTuple> first = arr.get(0);
		arr.remove(0);
		JavaPairDStream<String,LRTuple> all = ssc.union(first, arr);
		JavaPairDStream<String,LRTuple> segmentStream = generatePositionStream(all); 

		JavaPairDStream<String, Tuple2<Boolean, Long>> accidentStream = segmentStream
				.groupByKey(new HashPartitioner(LinearRoadMain.currentPartitions)).transformToPair(
						new Function2<JavaPairRDD<String, Iterable<LRTuple>>, Time, JavaPairRDD<String, Tuple2<Boolean, Long>>>() {
							private static final long serialVersionUID = -9202502146928153066L;
							@Override
							public JavaPairRDD<String, Tuple2<Boolean, Long>> call(
									JavaPairRDD<String, Iterable<LRTuple>> v1, Time v2) throws Exception {
								JavaPairRDD<String, Tuple2<Optional<Iterable<LRTuple>>, Optional<AccidentSegmentState> >> tmpRdd = v1
										.fullOuterJoin(accidentState);
								JavaPairRDD<String, AccidentSegmentState> res = tmpRdd.mapToPair(new DetectAccidents())
										.persist(StorageLevel.MEMORY_AND_DISK());
								res.count();
								accidentState.unpersist(false);
								accidentState = res;
								if (checkpointCounter > maxCheckpointCounter) {
									accidentState.checkpoint();
									checkpointCounter = 0;
								} else
									checkpointCounter = checkpointCounter + 1;
								return v1.join(accidentState).
										repartitionAndSortWithinPartitions(new HashPartitioner(LinearRoadMain.currentPartitions)).
										flatMapToPair(new OutputAccidentSegments());
							}
						});
		
		JavaPairDStream<String, Tuple2<Tuple2<Iterable<LRTuple>, Optional<Tuple2<Boolean, Long>>>,TollSegmentState>> alltuplesTmp = segmentStream.
				groupByKey(new HashPartitioner(LinearRoadMain.currentPartitions))
				.leftOuterJoin(accidentStream).transformToPair(
						new Function2<JavaPairRDD<String, Tuple2<Iterable<LRTuple>, Optional<Tuple2<Boolean, Long>>>>, Time, 
							JavaPairRDD<String, Tuple2<Tuple2<Iterable<LRTuple>, Optional<Tuple2<Boolean, Long>>>,TollSegmentState>>>() {
							private static final long serialVersionUID = -9202502146928153066L;
							@Override
							public JavaPairRDD<String, Tuple2<Tuple2<Iterable<LRTuple>, Optional<Tuple2<Boolean, Long>>>,TollSegmentState>> call(
									JavaPairRDD<String, Tuple2<Iterable<LRTuple>, Optional<Tuple2<Boolean, Long>>>> v1, Time v2)
											throws Exception {
								JavaPairRDD<String, Tuple2<Optional<Tuple2<Iterable<LRTuple>, Optional<Tuple2<Boolean, Long>>>>, 
									Optional<TollSegmentState>>> tmpRdd = v1.fullOuterJoin(tollState);
								JavaPairRDD<String, TollSegmentState> res = tmpRdd.mapToPair(new ComputeTolls())
										.persist(StorageLevel.MEMORY_AND_DISK()).repartition(LinearRoadMain.currentPartitions);
								res.count();
								tollState.unpersist(false);
								// we update the accident state here
								tollState = res;
								if (checkpointCounter == 0) {
									tollState.checkpoint();
								}
								// the first one was to update the state, now we
								// merge with the new state and we output the
								// accident segments
								return v1.join(res);
							}
						});
		JavaPairDStream<String, Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>> alltuplesTollAccident 
			= alltuplesTmp.flatMapToPair(new OutputTollsAndAccidents());
		
		JavaDStream<Tuple6<Integer, Integer, Long, Long, Integer, Integer>> accidentTuples = alltuplesTollAccident
				.filter(new Function<Tuple2<String,Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>>, Boolean>() {
					@Override
					public Boolean call(Tuple2<String,Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>> value)
							throws Exception {
						if (value._2()._1() == 1)
							return true;
						return false;
					}
				})
				.map(new Function<Tuple2<String,Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>>, 
						Tuple6<Integer, Integer, Long, Long, Integer, Integer>>() {
					@Override
					public Tuple6<Integer, Integer, Long, Long, Integer, Integer> call(
							Tuple2<String,Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>> val) throws Exception {
						Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double> value = val._2;
						return new Tuple6<Integer, Integer, Long, Long, Integer, Integer>(value._1(), value._2(),
								value._3(), System.currentTimeMillis(), value._5(), value._6());
					}
				});
		JavaDStream<Tuple6<Integer, Integer, Long, Long, Integer, Integer>> tollTuples = alltuplesTollAccident
				.filter(new Function<Tuple2<String,Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>>, Boolean>() {
					@Override
					public Boolean call(Tuple2<String,Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>> val)
							throws Exception {
						Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double> value = val._2;
						if (value._1() == 0)
							return true;
						return false;
					}
				})
				.map(new Function<Tuple2<String,Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>>, 
						Tuple6<Integer, Integer, Long, Long, Integer, Integer>>() {
					@Override
					public Tuple6<Integer, Integer, Long, Long, Integer, Integer> call(
							Tuple2<String,Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>> val) throws Exception {
						Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double> value = val._2;
						return new Tuple6<Integer, Integer, Long, Long, Integer, Integer>(value._1(), value._2(),
								value._3(), System.currentTimeMillis(), value._6().intValue(), value._7().intValue());
					}
				});
		accidentTuples.foreachRDD(new AccidentPrinter());
		tollTuples.foreachRDD(new TollPrinter());
		
		ssc.start();
		ssc.awaitTermination();
	}

	public static JavaPairDStream<String, LRTuple> parseInitialTuples(JavaStreamingContext ssc, String host, Integer port) {
		JavaReceiverInputDStream<String> lines = ssc.socketTextStream(host, port, StorageLevels.MEMORY_AND_DISK);
		return lines.transformToPair(new Function<JavaRDD<String>, JavaPairRDD<String, LRTuple>>() {
			@Override
			public JavaPairRDD<String, LRTuple> call(JavaRDD<String> v1) throws Exception {
				JavaPairRDD<String, LRTuple> res = v1.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, LRTuple>() {
					@Override
					public Iterable<Tuple2<String,LRTuple>> call(Iterator<String> x) {
						// return a tuple identified by key
						// Type = 0, Time, VID, Spd, XWay, Lane, Dir, Seg, Pos
						List<Tuple2<String,LRTuple>> res = new ArrayList<Tuple2<String,LRTuple>>();
						Iterator<String> it = x;
						while (it.hasNext()) {
							String elem = it.next();
							LRTuple t = new LRTuple(elem);
							String key = t.xway+"_"+t.seg;
							res.add(new Tuple2<String, LRTuple>(key,t));
						}
						return res;
					}
				});
				return res.repartition(LinearRoadMain.currentPartitions);
			}
		});
	}

	public static JavaPairDStream<String,LRTuple> generatePositionStream(JavaPairDStream<String,LRTuple> tupleDstream) {
		JavaPairDStream<String,LRTuple> positionStream = tupleDstream.filter(new Function<Tuple2<String,LRTuple>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String,LRTuple> x) {
				if (x._2.type == 0)
					return true;
				return false;
			}
		});
		// positionStream.cache();
		return positionStream;
	}


	public static JavaPairDStream<Integer, LRTuple> generateVidStream(JavaPairDStream<String,LRTuple> positionStream) {
		// prepare for checking accidents in segment
		// here I repartition the data
		JavaPairDStream<Integer, LRTuple> vidStream = positionStream
				.transformToPair(new Function<JavaPairRDD<String,LRTuple>, JavaPairRDD<Integer, LRTuple>>() {
					@Override
					public JavaPairRDD<Integer, LRTuple> call(JavaPairRDD<String,LRTuple> s) {

						JavaPairRDD<Integer, LRTuple> res = s.mapToPair(new PairFunction<Tuple2<String,LRTuple>, Integer, LRTuple>() {
							@Override
							public Tuple2<Integer, LRTuple> call(Tuple2<String,LRTuple> s1) {
								return new Tuple2<Integer, LRTuple>(s1._2.vid, s1._2);
							}
						}).repartitionAndSortWithinPartitions(new HashPartitioner(LinearRoadMain.vidPartitions));
						res.cache();
						return res;
					}
				});
		return vidStream;
	}

	private static class OutputAccidentSegments implements
			PairFlatMapFunction<Tuple2<String, Tuple2<Iterable<LRTuple>, AccidentSegmentState>>, String, Tuple2<Boolean, Long>> {

		@Override
		public Iterable<Tuple2<String, Tuple2<Boolean, Long>>> call(
				Tuple2<String, Tuple2<Iterable<LRTuple>, AccidentSegmentState>> t) throws Exception {
			ArrayList<Tuple2<String, Tuple2<Boolean, Long>>> collector = new ArrayList<Tuple2<String, Tuple2<Boolean, Long>>>();
			boolean newAcc = t._2()._2().isNewAccident();
			boolean newClear = t._2()._2().isCleared();
			System.out.println("Output Accident Segments: "+t._2()._2().toString());
			if (newAcc) {
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
						collector.add(new Tuple2<String, Tuple2<Boolean, Long>>(xway + "_" + realseg,
								new Tuple2(false, time)));
					}
				}
			}
			return collector;
		}

	}
	
	private static class SharedCount extends Thread {
		private static AtomicInteger count = new AtomicInteger(0);
		public SharedCount() {
			
		}
		
		@Override
		public void run() {
			while(true) {
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
					continue;
				}
				count.incrementAndGet();
			}
		}

		public static int getCount() {
			return count.get();
		}

		public static void setCount(int count) {
			SharedCount.count.set(count);
		}
	}

	private static class DetectAccidents implements
			PairFunction<Tuple2<String, Tuple2<Optional<Iterable<LRTuple>>, 
			Optional<AccidentSegmentState>>>, String, AccidentSegmentState> {
		private static final long serialVersionUID = 11625252L;
		
		@Override
		public Tuple2<String, AccidentSegmentState> call(
				Tuple2<String, Tuple2<Optional<Iterable<LRTuple>>, Optional<AccidentSegmentState>>> v) throws Exception {
			AccidentSegmentState newState; // the segment state for this tuple
			if (v._2._2.isPresent()) {
				newState = v._2._2.get();
			} else {
				newState = new AccidentSegmentState();
			}
			if(v._2._1.isPresent()) {
				for (LRTuple t : v._2._1.get()) {
						// we update the state ?
						// VID, Spd, XWay, Lane, Dir, Seg, Pos
						newState.updateSegmentState(t.vid, t.time, t.xway, t.lane, t.seg, t.pos, t.speed);
				}
			}
			// we purge vehicles for which we did not receive a notification in the last 30 seconds
			return new Tuple2<String, AccidentSegmentState>(v._1, newState);
		}
	}

	private static class ComputeTolls implements
			PairFunction<Tuple2<String, Tuple2<Optional<Tuple2<Iterable<LRTuple>, 
			Optional<Tuple2<Boolean, Long>>>>, Optional<TollSegmentState>>>, 
			String, TollSegmentState> {
		
		private static final long serialVersionUID = 199484848L;
		private transient int sharedCount;
		{
			sharedCount = 0;
		}
		
		@Override
		public Tuple2<String, TollSegmentState> call(
				Tuple2<String, Tuple2<Optional<Tuple2<Iterable<LRTuple>, Optional<Tuple2<Boolean, Long>>>>, 
				Optional<TollSegmentState>>> value)
						throws Exception {
			TollSegmentState newState;
			if (value._2._2.isPresent()) {
				newState = value._2._2.get();
			} else {
				newState = new TollSegmentState();
			}
			
			if(sharedCount > 50) {
				sharedCount = 0;
				long time = System.currentTimeMillis();
				ArrayList<Integer> toRemove = new ArrayList<Integer>();
				HashMap<Integer, Long> segments = newState.getPreviousSegments();
				for(Integer vt : segments.keySet()) {
						if(time - segments.get(vt) > 30000) {
							toRemove.add(vt);
						}
					}
					for(Integer vt : toRemove)
						segments.remove(vt);
			   } else {
				   sharedCount++;
			   }
			
			if(value._2()._1.isPresent()) {
				Iterator<LRTuple> tit = value._2()._1().get()._1().iterator();
				// VID, Spd, XWay, Lane, Dir, Seg, Pos
				while(tit.hasNext()) {
					LRTuple t = tit.next();
					newState.isNew(t.vid, t.time);
					newState.computeTolls(t.time, t.vid, t.seg, t.lane, t.pos, t.speed);
				}
				// update accident info
				if (value._2()._1().get()._2().isPresent())
					newState.markAndClearAccidents(value._2()._1().get()._2().get());
			}
			return new Tuple2<String, TollSegmentState>(value._1(), newState);
		}
	}

	private static class OutputTollsAndAccidents implements
			PairFlatMapFunction<Tuple2<String, Tuple2<Tuple2<Iterable<LRTuple>, Optional<Tuple2<Boolean, Long>>>, TollSegmentState>>, 
			String, Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>> {
		
		@Override
		public Iterable<Tuple2<String, Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>>> call(
				Tuple2<String, Tuple2<Tuple2<Iterable<LRTuple>, Optional<Tuple2<Boolean, Long>>>, TollSegmentState>> value)
						throws Exception {
			ArrayList<Tuple2<String, Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>>> collector = 
					new ArrayList<Tuple2<String, Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>>>();
			TollSegmentState cstate = value._2()._2();
			Iterator<LRTuple> tit = value._2._1._1.iterator();
			String segid = value._1();
			int sspeed = (int) cstate.getLav();
			while(tit.hasNext()) {
				LRTuple t = tit.next();
				if(!cstate.getPreviousSegments().containsKey(t.vid)) // we don't output for vids which did not change segments
					continue;
				long time = t.time;
				int lane = t.lane;
				int vid = t.vid;
				int segment = t.seg;
				int position = t.pos;
				boolean outputAcc = cstate.needToOutputAccident(time, lane);
				if (outputAcc) {
					collector.add(new Tuple2<String, Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>>(segid,
						new Tuple7(1, vid, time, System.currentTimeMillis(), segment, position, 0.0)));
				}
				double vtoll = cstate.getCurrentToll(segid, outputAcc);
				collector.add(new Tuple2(segid, new Tuple7<Integer, Integer, Long, Long, Integer, Integer, Double>(0, vid,
					time, System.currentTimeMillis(), segment, sspeed, vtoll)));
			}
			// purge old vehicles for which we didn't receive a notification in the last 30 seconds...
			
			return collector;
		}
	}

	private static class OutputNewSegmentVids
			implements PairFunction<Tuple2<Integer, Tuple2<LRTuple, Optional<String>>>, String, Tuple2<LRTuple, Boolean>> {
		@Override
		public Tuple2<String, Tuple2<LRTuple, Boolean>> call(Tuple2<Integer, Tuple2<LRTuple, Optional<String>>> t)
				throws Exception {
			String currentSeg = "" + t._2._1.xway + "_" + t._2._1.seg;
			boolean res = false;
			if (t._2._2.isPresent()) {
				if (!t._2._2.get().equals(currentSeg)) {
					res = true;
				}
			} else {
				res = true;
			}
			return new Tuple2<String, Tuple2<LRTuple, Boolean>>(currentSeg, new Tuple2(t._2._1, res));
		}
	}

	private static class UpdatePreviousSegment
			implements PairFlatMapFunction<Tuple2<Integer, Tuple2<Optional<LRTuple>, Optional<String>>>, Integer, String> {
		@Override
		public Iterable<Tuple2<Integer, String>> call(Tuple2<Integer, Tuple2<Optional<LRTuple>, Optional<String>>> t) throws Exception {
			ArrayList<Tuple2<Integer, String>> collector = new ArrayList<Tuple2<Integer, String>>();
			int vid = t._1;
			String new_segment = null;
			if(t._2._1.isPresent()) {
				// we always replace the old with the new value
				new_segment = "" + t._2._1.get().xway + "_" + t._2._1.get().seg;
			} else if(t._2._2.isPresent()) {
				// we keep the old value
				new_segment = "" + t._2._2.get();
			}
			if(new_segment != null) {
				collector.add(new Tuple2<Integer, String>(vid, new_segment));
			}
			return collector;
		}
	}
	
	private static class InitTuplesPrinter implements Function<
	JavaRDD<LRTuple>, Void> {
	private transient PrintWriter outLogger = null;
	{
		try {
			outLogger = new PrintWriter(
					new BufferedWriter(new FileWriter("/home/hduser/initTuplesSpark.csv", true)));
			outLogger.flush();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	
	@Override
	public Void call(
			JavaRDD<LRTuple> v1)
					throws Exception {
		if (outLogger == null)
			return null;
		List<LRTuple> tuples = v1.collect();
		for (LRTuple e : tuples) {
				outLogger.println(e.toString());
			}
		outLogger.flush();
		return null;
	}
	}
	
	private static class VidTuplesPrinter implements Function<
	JavaPairRDD<Integer,LRTuple>, Void> {
	private transient PrintWriter outLogger = null;
	{
		try {
			outLogger = new PrintWriter(
					new BufferedWriter(new FileWriter("/home/hduser/vidTuplesSpark.csv", true)));
			outLogger.flush();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	
	@Override
	public Void call(
			JavaPairRDD<Integer,LRTuple> v1)
					throws Exception {
		if (outLogger == null)
			return null;
		List<Tuple2<Integer,LRTuple>> tuples = v1.collect();
		for (Tuple2<Integer,LRTuple> e : tuples) {
				outLogger.println(e.toString());
			}
		outLogger.flush();
		return null;
	}
	
}
	
	private static class TollTuplesPrinter implements Function<
		JavaPairRDD<String, Tuple2<Tuple2<Iterable<Tuple2<LRTuple, Boolean>>, Optional<Tuple2<Boolean, Long>>>,TollSegmentState>>, Void> {
		private transient PrintWriter outLogger = null;
		{
			try {
				outLogger = new PrintWriter(
						new BufferedWriter(new FileWriter("/home/hduser/tollTmpSpark.csv", true)));
				outLogger.flush();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		
		@Override
		public Void call(
				JavaPairRDD<String, Tuple2<Tuple2<Iterable<Tuple2<LRTuple, Boolean>>, Optional<Tuple2<Boolean, Long>>>, TollSegmentState>> v1)
						throws Exception {
			if (outLogger == null)
				return null;
			List<Tuple2<String, Tuple2<Tuple2<Iterable<Tuple2<LRTuple, Boolean>>, Optional<Tuple2<Boolean, Long>>>, TollSegmentState>>> tuples = v1.collect();
			for (Tuple2<String, Tuple2<Tuple2<Iterable<Tuple2<LRTuple, Boolean>>, Optional<Tuple2<Boolean, Long>>>, TollSegmentState>> e : tuples) {
				Iterator<Tuple2<LRTuple, Boolean>> it = e._2._1._1.iterator();
				while(it.hasNext()) {
					Tuple2<LRTuple, Boolean> i = it.next();
					outLogger.println(i.toString());
				}
			}
			outLogger.flush();
			return null;
		}
		
	}
	
	
	private static class NewVehiclesPrinter implements Function<JavaPairRDD<String, Tuple2<LRTuple, Boolean>>, Void> {
		private transient PrintWriter outLogger = null;

		{
			try {
				outLogger = new PrintWriter(
						new BufferedWriter(new FileWriter("/home/hduser/newVehiclesSpark.csv", true)));
				outLogger.flush();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			outLogger.flush();
		}

		@Override
		public Void call(JavaPairRDD<String, Tuple2<LRTuple, Boolean>> v1) throws Exception {
			// TODO Auto-generated method stub
			if (outLogger == null)
				return null;
			List<Tuple2<String, Tuple2<LRTuple, Boolean>>> tuples = v1.collect();
			for (Tuple2<String, Tuple2<LRTuple, Boolean>> e : tuples) {
				outLogger.println(e.toString());
			}
			outLogger.flush();
			return null;
		}
		
	}
	
	


	private static class AccidentPrinter
			implements Function<JavaRDD<Tuple6<Integer, Integer, Long, Long, Integer, Integer>>, Void> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 4048889437211893963L;
		private transient PrintWriter outLogger = null;

		{
			try {
				outLogger = new PrintWriter(
						new BufferedWriter(new FileWriter("/home/hduser/accidentTuplesSpark.csv", true)));
				outLogger.flush();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}

		@Override
		public Void call(JavaRDD<Tuple6<Integer, Integer, Long, Long, Integer, Integer>> v1) throws Exception {
			if (outLogger == null)
				return null;
			List<Tuple6<Integer, Integer, Long, Long, Integer, Integer>> tuples = v1.collect();
			for (Tuple6<Integer, Integer, Long, Long, Integer, Integer> e : tuples) {
				outLogger.println(e._1() + "," + e._2() + "," + e._3() + "," + e._4() + "," + e._5() + "," + e._6());
			}
			outLogger.flush();
			return null;
		}

	}

	private static class TollPrinter
			implements Function<JavaRDD<Tuple6<Integer, Integer, Long, Long, Integer, Integer>>, Void> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 240133268055954403L;
		private transient PrintWriter outLogger = null;

		{
			try {
				outLogger = new PrintWriter(
						new BufferedWriter(new FileWriter("/home/hduser/tollTuplesSpark.csv", true)));
				outLogger.flush();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}

		@Override
		public Void call(JavaRDD<Tuple6<Integer, Integer, Long, Long, Integer, Integer>> v1) throws Exception {
			if (outLogger == null)
				return null;
			List<Tuple6<Integer, Integer, Long, Long, Integer, Integer>> tuples = v1.collect();
			for (Tuple6<Integer, Integer, Long, Long, Integer, Integer> e : tuples) {
				outLogger.println(e._1() + "," + e._2() + "," + e._3() + "," + e._4() + "," + e._5() + "," + e._6());
			}
			outLogger.flush();
			return null;
		}

	}
}
