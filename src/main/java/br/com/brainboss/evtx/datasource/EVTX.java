package br.com.brainboss.evtx.datasource;

import org.apache.log4j.Logger;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class EVTX implements TableProvider {
    private static final Logger log = Logger.getLogger(EVTX.class);
    public EVTX() {
    }
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        log.debug("InferSchema pass");
        return null;
    }

    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        log.debug("getTable pass");
        return new EVTXTable(schema,properties);
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }
}