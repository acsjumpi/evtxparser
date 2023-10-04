package br.com.brainboss.evtx.datasource;

import br.com.brainboss.evtx.parser.MalformedChunkException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.spark.internal.Logging;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.logging.Level;

public class EVTXPartitionReaderFactory implements PartitionReaderFactory {
    private final StructType schema;
    private final SerializableConfiguration sConf;
    private static final Logger log = Logger.getLogger(EVTXPartitionReaderFactory.class);

    public EVTXPartitionReaderFactory(StructType schema, SerializableConfiguration sConf) {
        this.schema = schema;
        this.sConf = sConf;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        log.debug("createReader joined");
        try {
            return new EVTXPartitionReader((EVTXInputPartition) partition, schema, sConf);
        } catch (FileNotFoundException | URISyntaxException e) {
            log.debug(String.valueOf(e));
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (MalformedChunkException e) {
            throw new RuntimeException(e);
        }
        return null;
    }
}