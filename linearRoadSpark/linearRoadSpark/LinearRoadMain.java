package linearRoadSpark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class LinearRoadMain {
	protected static int batchInteval = 3000;
	protected static final Duration windowAverage = new Duration(300000); // 5 minutes
	protected static final Duration windowSlide = new Duration(batchInteval); // 3 seconds slide
	protected static final Duration windowCount = new Duration(60000);
	
	protected static int currentPartitions;
	protected static final int maxCheckpoint = 4;
	
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
		
		LinearRoad lr = new LinearRoad(ssc);
		
		JavaPairDStream<String, SegmentState> stateDstream=null;
		JavaPairDStream<Integer, Tuple2<Long, List<Integer>>> initTuples = lr.parseInitialTuples(ssc, args[0], args[1]);
		JavaPairDStream<Integer, Tuple2<Long, List<Integer>>> positionStream = lr.generatePositionStream(initTuples);
		JavaPairDStream<String, Tuple2<Long, List<Integer>>> segmentStream = lr.generateSegmentStream(positionStream);
		
		JavaPairDStream<String, SegmentState> data = lr.checkVehiclesLeftSegment(ssc, positionStream); 
		stateDstream = lr.checkAccidents(data, segmentStream);
		stateDstream.cache();
		lr.outputAccidents(ssc, segmentStream, positionStream, stateDstream);
		
		lr.outputTolls(positionStream, segmentStream, stateDstream);
		
		ssc.start();
		ssc.awaitTermination();
	}
}
