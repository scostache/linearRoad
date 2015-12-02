package org.myorg.lrspark;

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
		String[] keysplit = mykey.split("_");
		//return mykey.hashCode() % partitions;
		int partition = (int) Math.ceil((Integer.parseInt(keysplit[0]) + 
				(int) Math.ceil(Integer.parseInt(keysplit[1])/4)) % partitions);
		//System.out.println("Key: "+mykey+" partition: "+partition);
		return partition;
	}

	@Override
	public int numPartitions() {
		return partitions;
	}

}
