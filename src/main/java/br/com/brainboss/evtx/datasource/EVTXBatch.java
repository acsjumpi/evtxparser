package br.com.brainboss.evtx.datasource;

import br.com.brainboss.evtx.parser.ChunkHeader;
import br.com.brainboss.evtx.parser.FileHeader;
import br.com.brainboss.evtx.parser.FileHeaderFactory;
import br.com.brainboss.evtx.parser.MalformedChunkException;
import com.google.common.primitives.UnsignedInteger;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public class EVTXBatch implements Batch {
    private final StructType schema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;
    private String filename;
    private static final Logger log = Logger.getLogger(EVTXBatch.class);
    private static final UnsignedInteger MAX_PARTITIONS = UnsignedInteger.valueOf(32);

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
//        return new InputPartition[]{new EVTXInputPartition()};
        return createPartitions();
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        log.debug("createReaderFactory joined");
        log.debug("fileName "+filename);
        return new EVTXPartitionReaderFactory(schema, filename);
    }

    private InputPartition[] createPartitions(){
        List<InputPartition> partitions = new ArrayList<>();
        UnsignedInteger chunkCount = UnsignedInteger.ZERO;
        try {
            log.debug("CreatePartitions joined");
            log.debug("fileName"+this.filename);
            FileInputStream filereader = new FileInputStream(new File(this.filename));
            FileHeaderFactory fileheaderfactory = FileHeader::new;
            FileHeader fileheader = fileheaderfactory.create(filereader, log, false);
            chunkCount = fileheader.getChunkCount();

            int numPartitions = (chunkCount.compareTo(MAX_PARTITIONS) < 0 ? chunkCount : MAX_PARTITIONS).intValue();
            int groupSize = (chunkCount.dividedBy(UnsignedInteger.valueOf(numPartitions))).intValue();

            for(int i = 0; i < numPartitions; i++)
                partitions.add(new EVTXInputPartition(i, groupSize));

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return partitions.toArray(new InputPartition[chunkCount.intValue()]);
    }
}