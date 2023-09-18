package br.com.brainboss.evtx.datasource;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import org.apache.log4j.Logger;

public class EVTXTable implements SupportsRead {

    private final StructType schema;
    private final Map<String, String> properties;
    private Set<TableCapability> capabilities;
    private static final Logger log = Logger.getLogger(EVTXTable.class);

    public EVTXTable(StructType schema, Map<String, String> properties) {
        this.schema = schema;
        this.properties = properties;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        log.debug("newScanBuilder joined");
        return new EVTXScanBuilder(schema, properties, options);
    }

    @Override
    public String name() {
        return "dummy_table";
    }

    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        log.debug("Set::Capabilities joined");
        if (capabilities == null) {
            this.capabilities = new HashSet<>();
            capabilities.add(TableCapability.BATCH_READ);
//            capabilities.add(TableCapability.CONTINUOUS_READ);
        }
        return capabilities;
    }
}