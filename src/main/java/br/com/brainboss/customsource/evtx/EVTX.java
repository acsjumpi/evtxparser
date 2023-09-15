package br.com.brainboss.customsource.evtx;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class EVTX implements TableProvider {
    public EVTX() {

    }
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return null;
    }

    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new EVTXTable(schema,properties);
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }
}