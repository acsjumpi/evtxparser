package br.com.brainboss.evtx.datasource;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;
import org.apache.log4j.Logger;

public class EVTXBatch implements Batch {
    private final StructType schema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;
    private String filename;
    private static final Logger log = Logger.getLogger(EVTXBatch.class);
    public EVTXBatch(StructType schema,
                    Map<String, String> properties,
                    CaseInsensitiveStringMap options) {

        this.schema = schema;
        this.properties = properties;
        this.options = options;
        this.filename = options.get("fileName");
    }

    @Override
    public InputPartition[] planInputPartitions() {
        log.debug("planInputPartitions joined");
        return new InputPartition[]{new EVTXInputPartition()};
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        log.debug("createReaderFactory joined");
        log.debug("fileName "+filename);
        return new EVTXPartitionReaderFactory(schema, filename);
    }
}