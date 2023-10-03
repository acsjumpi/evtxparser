package br.com.brainboss.evtx.datasource;

import com.google.common.primitives.UnsignedInteger;
import org.apache.spark.sql.connector.read.InputPartition;

public class EVTXInputPartition implements InputPartition {

    private String filename;
    private final int partitionNumber;
    private final int groupSize;
    private final boolean last;

    public EVTXInputPartition(String filename, int partitionNumber, int groupSize, boolean last){
        this.filename = filename;
        this.partitionNumber = partitionNumber;
        this.groupSize = groupSize;
        this.last = last;
    }

    public String getFilename(){
        return filename;
    }

    public int getPartitionNumber(){
        return partitionNumber;
    }

    public UnsignedInteger getFirstChunk(){
        return UnsignedInteger.valueOf((partitionNumber) * groupSize);
    }

    public UnsignedInteger getLastChunk(){
        return UnsignedInteger.valueOf(((partitionNumber + 1) * groupSize) - 1);
    }

    public boolean isLast(){
        return last;
    }
    @Override
    public String[] preferredLocations() {
        return new String[0];
    }
}