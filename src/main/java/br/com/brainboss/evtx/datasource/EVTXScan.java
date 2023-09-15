package br.com.brainboss.evtx.datasource;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class EVTXScan implements Scan {
    private final StructType schema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;

    public EVTXScan(StructType schema,
                   Map<String, String> properties,
                   CaseInsensitiveStringMap options) {

        this.schema = schema;
        this.properties = properties;
        this.options = options;
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public String description() {
        return "evtx_scan";
    }

    @Override
    public Batch toBatch() {
        return new EVTXBatch(schema,properties,options);
    }
}