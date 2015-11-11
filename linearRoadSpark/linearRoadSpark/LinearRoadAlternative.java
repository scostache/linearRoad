package linearRoadSpark;

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
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


public class LinearRoadAlternative {
  private static final Pattern SPACE = Pattern.compile(",");
  private static final Pattern EOL = Pattern.compile("\n");
  
  private static final int batchInterval = 3;
  
  private static final Duration positionInterval = new Duration(120000);
  private static final Duration averageSpeedInterval = new Duration(300000);
  private static final Duration nbVehiclesInterval = new Duration(60000);
  
  static final Duration windowsl = new Duration(batchInterval*1000); // 1 second slide
  

  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: ReceiverExample <hostname> <port>");
      System.exit(1);
    }

    System.out.println(args[0]+" "+args[1]);
    
    // Create the context with a 1 second batch size
    SparkConf sparkConf = new SparkConf().setAppName("ReceiverExample");
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchInterval));
    ssc.checkpoint("hdfs://spark1:54310/checkpoint/");
    
    // Initial RDD input to updateStateByKey
    @SuppressWarnings("unchecked")
    List<Tuple2<String, SegmentStateAlternative>> tuples = Arrays.asList(new Tuple2<String, SegmentStateAlternative>("1_0", 
    		new SegmentStateAlternative()),
            new Tuple2<String, SegmentStateAlternative>("1_1", new SegmentStateAlternative()));
    JavaPairRDD<String, SegmentStateAlternative> globalSegmentState = ssc.sc().parallelizePairs(tuples);

    JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
            args[0], Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER_2);

    // we transform the tuples by type
    JavaDStream<Tuple2<Integer, Tuple2<Long, List<Integer>>>> tupleDstream = lines.filter(new Function<String, Boolean>() {
		@Override
		public Boolean call(String v1) throws Exception {
			if(v1.contentEquals(""))
				return false;
			return true;
		}
    }).map(new Function<String, Tuple2<Integer, Tuple2<Long,List<Integer>>>>() {
    	@Override
        public Tuple2<Integer, Tuple2<Long,List<Integer>>> call(String x) {
    		// return a tuple identified by key
    		//Type = 0, Time, VID, Spd, XWay, Lane, Dir, Seg, Pos
    		Long time = (long) System.currentTimeMillis()/1000;
    		ArrayList elems = Lists.newArrayList(SPACE.split(x.trim()));
    		Integer key = Integer.parseInt((String) elems.get(0));
    		elems.remove(0);
    		ArrayList<Integer> ints = new ArrayList<Integer>();
    		for(Object e : elems)
    			ints.add(Integer.parseInt((String)e));
          return new Tuple2(key, new Tuple2(time, ints));
        }
    });
    
    JavaDStream<Tuple2<Integer, Tuple2<Long,List<Integer>>>> positionStream = tupleDstream.filter(
    		new Function<Tuple2<Integer, Tuple2<Long,List<Integer>>>, Boolean> () {
    			@Override
    	        public Boolean call(Tuple2<Integer, Tuple2<Long,List<Integer>>> x) {
    				if (x._1 == 0)
    					return true;
    				return false;
    			}
    		 });
    
    JavaPairDStream<Integer, Iterable<List<Integer>>> positionsGrouped = positionStream.mapToPair(new PairFunction<Tuple2<Integer, 
    		Tuple2<Long,List<Integer>>>, Integer, List<Integer>>() {
    	@Override
        public Tuple2<Integer, List<Integer>> call(Tuple2<Integer, Tuple2<Long,List<Integer>>> s) {
    		ArrayList elems = new ArrayList();
    		elems.add(s._2._2.get(0)); // time
    		elems.add(s._2._2.get(3)); // highway
    		elems.add(s._2._2.get(6)); // segment
    		elems.add(s._2._2.get(7)); // position
    		return new Tuple2(s._2._2.get(1), elems); // group by vid
    	}
    }).groupByKeyAndWindow(LinearRoadAlternative.positionInterval);
    
    // identify vehicles which changed their segment
    JavaPairDStream<String, Integer> vehiclesChangedPosition = positionsGrouped.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, 
    		Iterable<List<Integer>>>, String, Integer>() {
		@Override
		public Iterable<Tuple2<String, Integer>> call(Tuple2<Integer, Iterable<List<Integer>>> t) throws Exception {
			ArrayList<Tuple2<String, Integer>> newSegment = new ArrayList();
			boolean changed_segment = false;
			Iterator<List<Integer>> it = t._2.iterator();
			List<Integer> last_highest_elem = it.next();
			if(!it.hasNext())
				return newSegment;
			List<Integer> highest_elem = it.next();
			if(last_highest_elem.get(0) > highest_elem.get(0)) {
				List<Integer> tm = highest_elem;
				highest_elem = last_highest_elem;
				last_highest_elem = tm;
			}
			List<Integer> elems = null;
			
			while (it.hasNext()) {
				elems = it.next();
				Integer time2 = elems.get(0);
				if(time2 > highest_elem.get(0)) {
					last_highest_elem = highest_elem;
					highest_elem = elems;
				}
				else if(time2 > last_highest_elem.get(0))
					last_highest_elem = elems;
			}
			String segstr = ""+last_highest_elem.get(1)+'_'+last_highest_elem.get(2);
			Integer time = last_highest_elem.get(0);
			Integer pos = last_highest_elem.get(3);
			String segstr2 = ""+highest_elem.get(1)+'_'+highest_elem.get(2);
			if(last_highest_elem.get(3) == highest_elem.get(3))
				return newSegment;
			newSegment.add(new Tuple2(segstr, t._1));
			return newSegment;
		}});
    vehiclesChangedPosition.updateStateByKey(updateFunction0, new HashPartitioner(ssc.sc().defaultParallelism()), globalSegmentState);
    // identify stopped vehicles
    JavaPairDStream<String, Tuple2<Integer, Integer>> stoppedVehicles = positionsGrouped.flatMapToPair(new 
    		PairFlatMapFunction<Tuple2<Integer, Iterable<List<Integer>>>, String, Tuple2<Integer,Integer>>() {
		@Override
		public Iterable<Tuple2<String, Tuple2<Integer,Integer>>> call(Tuple2<Integer, Iterable<List<Integer>>> t) throws Exception {
			ArrayList<Tuple2<String, Tuple2<Integer,Integer>>> newvidGroup = new ArrayList();
			boolean changed_segment = false;
			ArrayList<Tuple2<String, Tuple2<Integer,Integer>>> stoopedInfo = new ArrayList();
			Iterator<List<Integer>> it = t._2.iterator();
			List<Integer> elems = it.next();
			String segstr = ""+elems.get(1)+'_'+elems.get(2);
			Integer time = elems.get(0);
			Integer pos = elems.get(3);
			// we check if the previous segment is the same with the current segment
			int npos = 0;
			while (it.hasNext()) {
				elems = it.next();
				if (elems.get(3) == pos)
					npos ++;
				else
					npos = 0;
			}
			if(npos >=4) {
				stoopedInfo.add(new Tuple2(segstr, new Tuple2(t._1, pos)));
			}
			// we check if the 
			return stoopedInfo;
		}
    });
    JavaPairDStream<String, SegmentStateAlternative> stateDstream = stoppedVehicles.updateStateByKey(updateFunction1, 
    								new HashPartitioner(ssc.sc().defaultParallelism()), globalSegmentState);
      
    JavaPairDStream<String, Tuple2<Long,List<Integer>>> segmentStream = positionStream.mapToPair(
    		new PairFunction<Tuple2<Integer, Tuple2<Long,List<Integer>>>, String, Tuple2<Long,List<Integer>>>() {
    	          @Override
    	          public Tuple2<String, Tuple2<Long,List<Integer>>> call(Tuple2<Integer,Tuple2<Long,List<Integer>>> s) {
    	        	String seg = s._2._2.get(3).toString()+'_'+s._2._2.get(6).toString();
    	            return new Tuple2<String, Tuple2<Long,List<Integer>>>(seg, s._2);
    	          }
    	        });
    segmentStream.cache();
    JavaPairDStream<String, Tuple2<Tuple2<Long, List<Integer>>, SegmentStateAlternative>> currentsegmentStream =
    		segmentStream.join(stateDstream);
    

    JavaPairDStream<String, Boolean> accidentStream = currentsegmentStream.flatMapToPair(new 
    		PairFlatMapFunction<Tuple2<String, Tuple2<Tuple2<Long, List<Integer>>, SegmentStateAlternative>>, String, Tuple2<Long,Boolean>>() {
				@Override
				public Iterable<Tuple2<String, Tuple2<Long,Boolean>>> call(Tuple2<String, 
									Tuple2<Tuple2<Long, List<Integer>>, SegmentStateAlternative>> t) throws Exception {
					List<Tuple2<String, Tuple2<Long,Boolean>>> newList = new ArrayList<Tuple2<String, Tuple2<Long,Boolean>>>();
					
					String xway = t._1.split("_")[0];
					Integer segid = Integer.parseInt(t._1.split("_")[1]);
					Long time = System.currentTimeMillis()/1000;
					//System.out.println("Generating upstream accidents for key: "+t._1);
					if(t._2._2.isAccident()) {
						// we generate true for upstream events
						for(int i = 0; i<4; i++) {
							int newseg = segid - i;
							if(newseg < 0)
								continue;
							String key = xway +"_"+ newseg;
							newList.add(new Tuple2(key, new Tuple2(time,true)));
						}
					} else if (t._2._2.minuteCleared > 0) {
						for(int i = 0; i<4; i++) {
							int newseg = segid - i;
							if(newseg < 0)
								continue;
							String key = xway +"_"+ newseg;
							newList.add(new Tuple2(key, new Tuple2(time,false)));
						}
					}
					return newList;
				}}).reduceByKey(new Function2<Tuple2<Long,Boolean>, Tuple2<Long,Boolean>, Tuple2<Long,Boolean>>() {
					@Override
					public Tuple2<Long,Boolean> call(Tuple2<Long,Boolean> v1, Tuple2<Long,Boolean> v2) throws Exception {
						// we eliminate duplicates
						return new Tuple2(v1._1, v1._2 || v2._2);
					}
				}).mapToPair(new PairFunction<Tuple2<String, Tuple2<Long, Boolean>>, String, Boolean>() {
    			@Override
    				public Tuple2<String, Boolean> call(Tuple2<String, Tuple2<Long, Boolean>> t) throws Exception {
    					return new Tuple2(t._1, t._2._2);
    			}
    });
    JavaPairDStream<String, SegmentStateAlternative> updatedAccidentStream = accidentStream.updateStateByKey(updateFunction2, 
    											new HashPartitioner(ssc.sc().defaultParallelism()), globalSegmentState);
    
    
    JavaPairDStream<String, Integer> accidentTimeStream = currentsegmentStream.mapToPair(new PairFunction<Tuple2<String, 
    		Tuple2<Tuple2<Long, List<Integer>>,SegmentStateAlternative>>, String, Integer>()
    		{
    		@Override
    		public Tuple2<String, Integer> call(Tuple2<String, Tuple2<Tuple2<Long, List<Integer>>,SegmentStateAlternative>> t) throws Exception {
    			return new Tuple2(t._1, t._2._2.minuteAccident.intValue());
    		}
    		});
    
    segmentStream.join(updatedAccidentStream).filter(new Function<Tuple2<String, Tuple2<Tuple2<Long,List<Integer>>, SegmentStateAlternative>>, Boolean>() {
		@Override
		public Boolean call(Tuple2<String, Tuple2<Tuple2<Long,List<Integer>>, SegmentStateAlternative>> v1) throws Exception {
			return v1._2._2.isAccident() || v1._2._2.isUpstreamAccident();
		}
    }).join(accidentTimeStream).foreachRDD(new Function<JavaPairRDD<String,Tuple2<Tuple2<Tuple2<Long,List<Integer>>, SegmentStateAlternative>, Integer>>, Void>() {
		@Override
		public Void call(JavaPairRDD<String, Tuple2<Tuple2<Tuple2<Long,List<Integer>>, SegmentStateAlternative>, Integer>> v1) throws Exception {
			
			for(Tuple2<String, Tuple2<Tuple2<Tuple2<Long,List<Integer>>, SegmentStateAlternative>, Integer>> elem : v1.collect()) {
				List<Integer> s = elem._2._1._1._2;
				long time = elem._2._1._1._1;
				if (((long) s.get(s.size()-1)/60000)+1 >= elem._2._2 + 1)
						System.out.println("1"+","+s.get(1)+","+time+","+ System.currentTimeMillis()/1000 +
							","+s.get(6)+","+s.get(3));
				}
			return null;
		}
    	
    });
     
    
    // compute windowed avg speed per segmentStream
    /*
    JavaPairDStream<String, Double> avgbySegment = segmentStream.mapToPair(new PairFunction<
    		Tuple2<String, Tuple2<Long,List<Integer>>>, String, Tuple2<Integer,Integer> >() {
				@Override
				public Tuple2<String, Tuple2<Integer, Integer>> call(
						Tuple2<String, Tuple2<Long,List<Integer>>> t)
						throws Exception {
					return new Tuple2(t._1+"_"+t._2._2.get(1), new Tuple2(t._2._2.get(2), 1));
				}}).reduceByKeyAndWindow(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, 
						Tuple2<Integer, Integer>>() {
					@Override
					public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
						//System.out.println(""+v1._1+" "+v1._2+" "+v2._1+" "+v2._2);
						return new Tuple2(v1._1+v2._1, v1._2+v2._2);
					}}, ReceiverExample.averageSpeedInterval).mapToPair(new 
    	    		PairFunction<Tuple2<String, Tuple2<Integer, Integer>>, String, 
        			Tuple2<Integer, Double>>() {
    				@Override
    				public Tuple2<String, Tuple2<Integer, Double>> call(Tuple2<String,Tuple2<Integer, Integer>> v1) throws Exception {
    					//System.out.println(v1._1+" "+v1._2._1+" "+ v1._2._2);
    					String[] segmentl = v1._1.split("_");
    					String segment = segmentl[0]+"_"+segmentl[1];
    					return new Tuple2(segment, new Tuple2(1, new Double(v1._2._1/v1._2._2)));
    				}}).reduceByKey(new Function2<Tuple2<Integer,Double>, 
						Tuple2<Integer,Double>,
						Tuple2<Integer, Double>> () {
							@Override
							public Tuple2<Integer,Double> call(
									Tuple2<Integer,Double> v1,
									Tuple2<Integer,Double> v2) throws Exception {
								double speed = 0;
								int count = 0;
								count = v1._1 + v2._1;
								speed = v1._2 + v2._2;
								return new Tuple2(count, speed);
					}
				}).mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, Double>>, String, Double>() {
				@Override
				public Tuple2<String, Double> call(Tuple2<String, Tuple2<Integer, Double>> t) throws Exception {
					//System.out.println("xxx: "+t._1+" "+ t._2._1+" "+t._2._2);
					return new Tuple2(t._1, new Double(t._2._2/t._2._1));
				}
    		});
  
    JavaPairDStream<String, Integer> mincarsbySegment = segmentStream.mapToPair(new 
    		PairFunction<Tuple2<String, Tuple2<Long,List<Integer>>>, String, String>() {
				@SuppressWarnings("unchecked")
				@Override
				public Tuple2<String, String> call(Tuple2<String, Tuple2<Long,List<Integer>>> v1) throws Exception {
					return new Tuple2(v1._1, v1._1+"_"+v1._2._2.get(1));
				}}).countByValueAndWindow(ReceiverExample.nbVehiclesInterval, windowsl).
    		mapToPair(new PairFunction<Tuple2<Tuple2<String,String>, Long>, String, Integer>(){
					@Override
					public Tuple2<String, Integer> call(Tuple2<Tuple2<String, String>, Long> v1) throws Exception {
						return new Tuple2(v1._1._1, 1);
					}
				}).
    			reduceByKey(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1+v2;
					}});
    
    JavaPairDStream<String, Tuple2<Tuple2<Long, List<Integer>>, SegmentState>> joinedstateDstream =
    		segmentStream.join(stateDstream);
    
    JavaPairDStream<String, Tuple2<Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Double>, Integer>> avgperSegmentState = 
    		joinedstateDstream.join(avgbySegment).join(mincarsbySegment);
    // we need to send notifications to vehicles which are new in this stream
    JavaPairDStream<String, Tuple2<Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Double>, Integer>> newstateDstream = 
    		avgperSegmentState.filter(new 
    		Function<Tuple2<String, Tuple2<Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Double>, Integer>>, Boolean>() {
				@Override
				public Boolean call(Tuple2<String, Tuple2<Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Double>, Integer>> v1) 
						throws Exception {
					// I can compute the toll here, lol
					SegmentState s = v1._2._1._1._2;
				
					return s.hasNewVehicles();
				}
    });
    
    
    newstateDstream.foreachRDD(new Function2<JavaPairRDD<String, 
    		Tuple2<Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Double>, Integer>>, Time, Void>() {
				@Override
				public Void call(JavaPairRDD<String,Tuple2<Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Double>, Integer>> v1, Time v2)
						throws Exception {
					for(Tuple2<String,Tuple2<Tuple2<Tuple2<Tuple2<Long, List<Integer>>, SegmentState>, Double>, Integer>> elem : v1.collect()) {
						SegmentState s = elem._2._1._1._2;
						boolean hasAccident = s.hasAccident() || s.isUpstreamAccident();
						double avgspeed = elem._2._1._2;
						int nvehicles = elem._2._2;
						double toll = 0;
						if(hasAccident) {
							toll = 0;
						} else {
							if(nvehicles > 50 && avgspeed <40)
								toll = 2*(nvehicles-50)*(nvehicles-50);
						}
						for(Vehicle v : s.currentVehicles.values()) {
							if(v.isNew && v.lane != 4)
								System.out.println("0"+","+v.id+","+ v.time+","+
									System.currentTimeMillis()/1000+
									","+(int) avgspeed+","+nvehicles+","+(int) toll);
						}
					}
					return null;
				}
    }); 
    */
    
    //accidentTimeStream.print(100);
    //accidentStream.print(100);
    //lines.print(10);
    //avgbySegment.print(100);
    // here the processing of the stream starts
    //positionStream.print(10);
   // avgspeedbySegment.print();
    //stateDstream.print(100);
    vehiclesChangedPosition.print();
    stateDstream.print();
    stoppedVehicles.print();
    //newstateDstream.print(100);
    //tspeedbySegment.print(100);
    //tcarsbySegment.print(100);
    //mincarsbySegment.print(100);
    //joinedstateDstream.print(100);
    ssc.start();
    ssc.awaitTermination();
  }
  
  final static Function2<List<Integer>, Optional<SegmentStateAlternative>, Optional<SegmentStateAlternative>> updateFunction0 =
	        new Function2<List<Integer>, Optional<SegmentStateAlternative>, Optional<SegmentStateAlternative>>() {
	          @Override
	          public Optional<SegmentStateAlternative> call(List<Integer> values, Optional<SegmentStateAlternative> state) {
	        	  SegmentStateAlternative newState;
	        	  if(state.isPresent()) {
	        		  newState = state.get();
	        	  }
	        	  else {
	        		  newState =  new SegmentStateAlternative();
	        		  return Optional.of(newState);
	        	}
	        	// vehicle changed position, we remove it from here
	        	for(Integer vid : values) {
		        	  Tuple2<Integer, Integer> v = newState.isInStoppedVehicles(vid);
		        	  if(v == null)
		        		  continue;
		        	  newState.removeVehicle(v);
	        	  }
	        	  return Optional.of(newState);
	          }
};
  
  final static Function2<List<Boolean>, Optional<SegmentStateAlternative>, Optional<SegmentStateAlternative>> updateFunction2 =
	        new Function2<List<Boolean>, Optional<SegmentStateAlternative>, Optional<SegmentStateAlternative>>() {
	          @Override
	          public Optional<SegmentStateAlternative> call(List<Boolean> values, Optional<SegmentStateAlternative> state) {
	        	  SegmentStateAlternative newState;
	        	  if(state.isPresent()) {
	        		  newState = state.get();
	        	  }
	        	  else {
	        		  newState =  new SegmentStateAlternative();
	        	}
	        	if(newState.isAccident())
	        		return Optional.of(newState);
	        	boolean accident = false;
	        	for (Boolean v : values)
	        		accident = accident || v;
	        	if(accident && !newState.isAccident())
	        		newState.setUpstreamAccident(true);
	        	else if (newState.isUpstreamAccident() && !accident)
	        		newState.setUpstreamAccident(false);
	        	  return Optional.of(newState);
	          }
  };
  
  final static Function2<List<Tuple2<Integer, Integer>>, Optional<SegmentStateAlternative>, Optional<SegmentStateAlternative>> updateFunction1 =
	        new Function2<List<Tuple2<Integer, Integer>>, Optional<SegmentStateAlternative>, Optional<SegmentStateAlternative>>() {
	          @Override
	          public Optional<SegmentStateAlternative> call(List<Tuple2<Integer, Integer>> values, Optional<SegmentStateAlternative> state) {
	        	  SegmentStateAlternative newState;
	        	  if(state.isPresent()) {
	        		  newState = state.get();
	        	  }
	        	  else {
	        		  newState =  new SegmentStateAlternative();
	        	}
	            for (Tuple2<Integer, Integer> t : values) {
	            	newState.addVehicle(t);
	            }
	            if(newState.checkAccident())
	            	newState.setAccident();
	            return Optional.of(newState);
	          }
	        };

}

