package br.com.brainboss.evtx.datasource;

import br.com.brainboss.evtx.parser.FileHeader;
import br.com.brainboss.evtx.parser.FileHeaderFactory;
import com.google.common.primitives.UnsignedInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.util.SerializableConfiguration;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EVTXBatch implements Batch {
    private final StructType schema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;
    private final String filename;
    private final int numPartitions;
    private static final Logger log = Logger.getLogger(EVTXBatch.class);
    private final SerializableConfiguration sConf;

    public EVTXBatch(StructType schema,
                     Map<String, String> properties,
                     CaseInsensitiveStringMap options) {

        this.schema = schema;
        this.properties = properties;
        this.options = options;
        this.filename = options.get("fileName");
        this.numPartitions = options.getInt("numPartitions", 0);

        Configuration conf = SparkContext.getOrCreate().hadoopConfiguration();
        sConf = new SerializableConfiguration(conf);
    }

    @Override
    public InputPartition[] planInputPartitions() {
        log.debug("planInputPartitions joined");
        return createPartitions();
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        log.debug("createReaderFactory joined");
        return new EVTXPartitionReaderFactory(schema, sConf, false);
    }

    private InputPartition[] createPartitions(){
        List<InputPartition> partitions = new ArrayList<>();
        UnsignedInteger chunkCount = UnsignedInteger.ZERO;
        int numPartitions = 0;
        try {
            log.debug("CreatePartitions joined");
            log.debug("fileName"+this.filename);
            Path filePath = new Path(filename);
            FileSystem fs = filePath.getFileSystem(sConf.value());
            FSDataInputStream filereader = fs.open(filePath);
            FileHeaderFactory fileheaderfactory = FileHeader::new;
            FileHeader fileheader = fileheaderfactory.create(filereader, log, false);
            chunkCount = fileheader.getChunkCount();

            numPartitions = this.numPartitions == 0 ? chunkCount.intValue() : this.numPartitions;
            log.debug("Received numPartitions: "+this.numPartitions);
            log.debug("After numPartitions definition: "+numPartitions);
            int groupSize = (chunkCount.dividedBy(UnsignedInteger.valueOf(numPartitions))).intValue();

            for(int i = 0; i < numPartitions; i++)
                partitions.add(new EVTXInputPartition(filePath, i, groupSize, i+1 == numPartitions));

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return partitions.toArray(new InputPartition[numPartitions]);
    }
}