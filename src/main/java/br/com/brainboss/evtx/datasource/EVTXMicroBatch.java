package br.com.brainboss.evtx.datasource;

import br.com.brainboss.evtx.parser.FileHeader;
import br.com.brainboss.evtx.parser.FileHeaderFactory;
import com.google.common.primitives.UnsignedInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.execution.streaming.LongOffset;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.util.SerializableConfiguration;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EVTXMicroBatch implements MicroBatchStream {
    private final StructType schema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;
    private final String dir;
    private final int numPartitions;
    private static final Logger log = Logger.getLogger(EVTXMicroBatch.class);
    private LongOffset lastOffsetCommitted = LongOffset.apply(-1);
    private List<FileStatus> unreadFiles = new ArrayList<>();
    private FileSystem fs;
    private final SerializableConfiguration sConf;

    public EVTXMicroBatch(StructType schema,
                          Map<String, String> properties,
                          CaseInsensitiveStringMap options) {

        this.schema = schema;
        this.properties = properties;
        this.options = options;
        this.dir = options.get("path");
        this.numPartitions = options.getInt("numPartitions", 0);

        Configuration conf = SparkContext.getOrCreate().hadoopConfiguration();
        sConf = new SerializableConfiguration(conf);
    }

    @Override
    public Offset latestOffset(){
        log.debug("latestOffset joined");
        if(unreadFiles.isEmpty()){
            List<FileStatus> allFiles = listFiles();
            unreadFiles = allFiles.stream()
                    .filter(fileStatus -> isFileWithExtension(fileStatus, ".evtx"))
//                    .map(FileStatus::getPath)
                    .collect(Collectors.toList());

        }

        return new LongOffset(unreadFiles.size());
    }

    @Override
    public InputPartition[] planInputPartitions(Offset startOffset, Offset endOffset) {
        log.debug("planInputPartitions joined");
//        return new InputPartition[]{new EVTXInputPartition()};
        long start = ((LongOffset)startOffset).offset() + 1;
        long end = ((LongOffset)endOffset).offset() + 1;

        int sliceStart = (int) (start - lastOffsetCommitted.offset() - 1);
        int sliceEnd = (int) (end - lastOffsetCommitted.offset() - 1);

        log.debug("start: " + start);
        log.debug("end: " + end);
        log.debug("slicestart: " + sliceStart);
        log.debug("sliceend: " + sliceEnd);
        log.debug("lastOffsetCommitted: " + lastOffsetCommitted.offset());
        log.debug("unreadFiles.size: " + unreadFiles.size());

        if(unreadFiles.isEmpty())
            return new InputPartition[0];

        FileStatus file = unreadFiles.remove((int) start);
        return createPartitions(file.getPath());
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        log.debug("createReaderFactory joined");
        return new EVTXPartitionReaderFactory(schema, sConf);
    }

    private static boolean isFileWithExtension(FileStatus fileStatus, String desiredExtension) {
        String fileName = fileStatus.getPath().getName();
        return fileName.endsWith(desiredExtension);
    }

    private List<FileStatus> listFiles(){
        Path path = new Path(dir);
//        Configuration conf = new Configuration();
        List<FileStatus> files;
        try{
//            FileSystem fs = FileSystem.get(conf);

            fs = path.getFileSystem(sConf.value());
            files = Arrays.stream(fs.listStatus(path)).collect(Collectors.toList());
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return files;
    }

    private InputPartition[] createPartitions(Path filename){
        List<InputPartition> partitions = new ArrayList<>();
        UnsignedInteger chunkCount = UnsignedInteger.ZERO;
        int numPartitions = 0;
        try {
            log.debug("CreatePartitions joined");
            log.debug("filename "+filename.toString());
//            FileInputStream filereader = new FileInputStream(new File(filename));
            FSDataInputStream filereader = fs.open(filename);
            FileHeaderFactory fileheaderfactory = FileHeader::new;
            FileHeader fileheader = fileheaderfactory.create(filereader, log, false);
            chunkCount = fileheader.getChunkCount();

            numPartitions = this.numPartitions == 0 ? chunkCount.intValue() : this.numPartitions;
            log.debug("Received numPartitions: "+this.numPartitions);
            log.debug("After numPartitions definition: "+numPartitions);
            int groupSize = (chunkCount.dividedBy(UnsignedInteger.valueOf(numPartitions))).intValue();

            for(int i = 0; i < numPartitions; i++)
                partitions.add(new EVTXInputPartition(filename, i, groupSize, i+1 == numPartitions));

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return partitions.toArray(new InputPartition[numPartitions]);
    }

    @Override
    public Offset initialOffset() {
        log.debug("initialOffset joined");
        return LongOffset.apply(-1);
    }

    @Override
    public Offset deserializeOffset(String json) {
        log.debug("deserializeOffset joined");
        return new LongOffset(Long.parseLong(json));
    }

    @Override
    public void commit(Offset end) {
        log.debug("commit joined");
        LongOffset newOffset = (LongOffset) end;

        long offsetDiff = (newOffset.offset() - lastOffsetCommitted.offset());

        lastOffsetCommitted = newOffset;
    }

    @Override
    public void stop() {
        log.debug("stop joined");
    }
}