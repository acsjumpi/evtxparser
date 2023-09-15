package br.com.brainboss.evtx.datasource;

import org.apache.spark.sql.connector.read.InputPartition;

public class EVTXInputPartition implements InputPartition {

    @Override
    public String[] preferredLocations() {
        return new String[0];
    }
}