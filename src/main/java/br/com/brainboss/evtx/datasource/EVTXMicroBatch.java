package br.com.brainboss.evtx.datasource;

import br.com.brainboss.evtx.parser.FileHeader;
import br.com.brainboss.evtx.parser.FileHeaderFactory;
import com.google.common.primitives.UnsignedInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.execution.streaming.LongOffset;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.util.SerializableConfiguration;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class EVTXMicroBatch implements MicroBatchStream {
    private final StructType schema;
    private final String dir;
    private final int numPartitions;
    private final int maxBatchSize;
    private static final Logger log = Logger.getLogger(EVTXMicroBatch.class);
    private LongOffset lastOffsetCommitted = LongOffset.apply(-1);
    private List<FileStatus> files = new ArrayList<>();
    private FileSystem fs;
    private final SerializableConfiguration sConf;
    private final boolean debugMode;

    public EVTXMicroBatch(StructType schema,
                          Map<String, String> properties,
                          CaseInsensitiveStringMap options) {

        this.schema = schema;
        this.dir = options.get("path");
        this.numPartitions = options.getInt("numPartitions", 0);
        this.maxBatchSize = options.getInt("batchSize", 1);
        this.debugMode = options.getBoolean("debugMode", false);

        Configuration conf = SparkContext.getOrCreate().hadoopConfiguration();
        sConf = new SerializableConfiguration(conf);
    }

    @Override
    public Offset latestOffset(){
        // lista com todos os arquivos
        // retorna o tamanho da lista, que foi incrementado com os ultimos arquivos lidos
        // ordenar lista por modificationTime
        log.debug("latestOffset joined");
        List<FileStatus> filteredFiles = listFiles();
        files = filteredFiles.stream()
                .sorted(Comparator.comparingLong(FileStatus::getModificationTime))
                .collect(Collectors.toList());

        long newFiles = files.size() - lastOffsetCommitted.offset() - 1;
        long end = newFiles <= maxBatchSize ? newFiles : maxBatchSize;

        return new LongOffset(end);
    }

    @Override
    public InputPartition[] planInputPartitions(Offset startOffset, Offset endOffset) {
        // endOffset = latestOffset
        // startOffset = ?
        // processar todos os arquivos nesse microBatch, do lastOffsetCommited até endOffset
        // TODO: parametro maxBatchSize para limitar a quantidade de arquivos por batch
        // criar uma lsita de arquivos ja processados
        log.debug("planInputPartitions joined");
        if(files.isEmpty())
            return new InputPartition[0];

        log.debug("startOffset: " + ((LongOffset) startOffset).offset());
        log.debug("endOffset: " + ((LongOffset) endOffset).offset());

        log.debug("lastOffsetCommitted: " + lastOffsetCommitted.offset());
        log.debug("unreadFiles.size: " + files.size());


        int start = ((int) lastOffsetCommitted.offset() + 1);
        int end = (int) ((LongOffset) endOffset).offset();

        List<InputPartition> partitions = new ArrayList<>();
        for(int i = start; i < end; i++){
            partitions.addAll(createPartitions(files.get(i).getPath()));
        }

        return partitions.toArray(new InputPartition[numPartitions]);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        log.debug("createReaderFactory joined");
        return new EVTXPartitionReaderFactory(schema, sConf, debugMode);
    }

    private List<FileStatus> listFiles(){
        Path path = new Path(dir);
        List<FileStatus> files;
        try{
            fs = path.getFileSystem(sConf.value());
            PathFilter filter = file -> file.getName().endsWith(".evtx");
            files = Arrays.stream(fs.listStatus(path, filter)).collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return files;
    }

    private List<InputPartition> createPartitions(Path filename){
        List<InputPartition> partitions = new ArrayList<>();
        UnsignedInteger chunkCount;
        int numPartitions = 0;
        try {
            log.debug("CreatePartitions joined");
            log.debug("filename "+filename.toString());
            FSDataInputStream filereader = fs.open(filename);
            FileHeaderFactory fileheaderfactory = FileHeader::new;
            FileHeader fileheader = fileheaderfactory.create(filereader, log, false);
            chunkCount = fileheader.getChunkCount();

            numPartitions = this.numPartitions == 0 ? chunkCount.intValue() : this.numPartitions;
            log.debug("Received numPartitions: "+this.numPartitions);
            int groupSize = (chunkCount.dividedBy(UnsignedInteger.valueOf(numPartitions))).intValue();

            for(int i = 0; i < numPartitions; i++)
                partitions.add(new EVTXInputPartition(filename, i, groupSize, i+1 == numPartitions));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return partitions;
    }

    @Override
    public Offset initialOffset() {
        // Chamado somente no inicio da execuçao para continuar um processo reiniciado
        // Retornar o offset salvo no checkpoint, caso exista
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
        // Chamado somento quando entra no proximo microBatch
        log.debug("commit joined");
        long lastOffsetCommitted = ((LongOffset) end).offset() - 1;
        this.lastOffsetCommitted = LongOffset.apply(lastOffsetCommitted);
    }

    @Override
    public void stop() {
        log.debug("stop joined");
    }
}