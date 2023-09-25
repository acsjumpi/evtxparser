package br.com.brainboss.evtx.datasource;

import com.google.common.primitives.UnsignedInteger;
import org.apache.spark.sql.connector.read.InputPartition;

public class EVTXInputPartition implements InputPartition {

    private final UnsignedInteger chunkNumber;
    public EVTXInputPartition(UnsignedInteger chunkNumber){
        this.chunkNumber = chunkNumber;
    }

    public UnsignedInteger getChunkNumber(){
        return chunkNumber;
    }
    @Override
    public String[] preferredLocations() {
        return new String[0];
    }
}