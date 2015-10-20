package linearRoadSpark;

import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;

public class SegmentPartitioner extends HashPartitioner {
	private int partitions;

	public SegmentPartitioner(int partitions) {
		super(partitions);
		this.partitions = partitions;
	}

	@Override
	public int getPartition(Object arg0) {
		String mykey = (String) arg0;
		String[] segmentl = mykey.split("_");
		String segment = segmentl[0] + "_" + segmentl[1];
		return segment.hashCode() % partitions;
	}

	@Override
	public int numPartitions() {
		return partitions;
	}

}
