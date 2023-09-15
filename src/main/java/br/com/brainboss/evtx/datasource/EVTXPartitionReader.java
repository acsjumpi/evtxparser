package br.com.brainboss.evtx.datasource;

import br.com.brainboss.evtx.parser.ChunkHeader;
import br.com.brainboss.evtx.parser.FileHeader;
import br.com.brainboss.evtx.parser.FileHeaderFactory;
import br.com.brainboss.evtx.parser.MalformedChunkException;
import br.com.brainboss.evtx.parser.Record;
import com.opencsv.CSVReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.logging.Logger;

public class EVTXPartitionReader implements PartitionReader<InternalRow> {

    private final EVTXInputPartition csvInputPartition;
    private final String fileName;
    private Iterator<String[]> iterator;
    private EVTXReader csvReader;
    private List<Function> valueConverters;
    private final FileHeaderFactory fileheaderfactory;
    private final Logger log;
    private FileHeader fileheader;
    private ChunkHeader chunkheader;

    public EVTXPartitionReader(
            EVTXInputPartition csvInputPartition,
            StructType schema,
            String fileName) throws FileNotFoundException, URISyntaxException {
        this.csvInputPartition = csvInputPartition;
        this.fileName = fileName;
        this.valueConverters = ValueConverters.getConverters(schema);
        this.log = Logger.getLogger(EVTXPartitionReader.class.getName());
        this.fileheaderfactory = FileHeader::new;
        this.createEvtxReader();
    }

    private void createEvtxReader() throws URISyntaxException, IOException, MalformedChunkException {
        FileInputStream filereader;
        URL resource = this.getClass().getClassLoader().getResource(this.fileName);
        filereader = new FileInputStream(new File(resource.toURI()));
        fileheader = fileheaderfactory.create(filereader, log);
        chunkheader = fileheader.next();
        Record record = chunkheader.next();

        csvReader = new CSVReader(filereader);
        iterator = csvReader.iterator();
        iterator.next();

    }

    @Override
    public boolean next() {
        return fileheader.hasNext();
    }

    @Override
    public InternalRow get() {
        while(fileheader.hasNext()) {
            chunkheader = fileheader.next();
            while(chunkheader.hasNext()) {
                Record record = chunkheader.next();
            }
        }



        Object[] values = iterator.next();
        Object[] convertedValues = new Object[values.length];
        for (int i = 0; i < values.length; i++) {
            convertedValues[i] = valueConverters.get(i).apply(values[i]);
        }
        return InternalRow.apply(JavaConverters.asScalaIteratorConverter(Arrays.asList(convertedValues).iterator()).asScala().toSeq());
    }

    @Override
    public void close() throws IOException {
        csvReader.close();
    }
}