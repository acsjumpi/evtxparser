package br.com.brainboss.evtx.datasource;

import org.apache.spark.sql.connector.read.InputPartition;

public class EVTXInputPartition implements InputPartition {

    private final int chunkNumber;
    public EVTXInputPartition(int chunkNumber){
        this.chunkNumber = chunkNumber;
    }

    public int getChunkNumber(){
        return chunkNumber;
    }
    @Override
    public String[] preferredLocations() {
        return new String[0];
    }
}