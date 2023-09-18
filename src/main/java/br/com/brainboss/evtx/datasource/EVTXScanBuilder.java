package br.com.brainboss.evtx.datasource;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;
import java.util.logging.Level;
import org.apache.log4j.Logger;

public class EVTXScanBuilder implements ScanBuilder {
    private final StructType schema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;
    private static final Logger log = Logger.getLogger(EVTXScanBuilder.class);

    public EVTXScanBuilder(StructType schema,
                          Map<String, String> properties,
                          CaseInsensitiveStringMap options) {

        this.schema = schema;
        this.properties = properties;
        this.options = options;
    }

    @Override
    public Scan build() {
        log.debug("Build joined");
        return new EVTXScan(schema,properties,options);
    }
}