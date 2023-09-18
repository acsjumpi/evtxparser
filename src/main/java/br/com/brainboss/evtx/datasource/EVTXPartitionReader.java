package br.com.brainboss.evtx.datasource;

import br.com.brainboss.evtx.handlers.RootNodeHandler;
import br.com.brainboss.evtx.handlers.RootNodeHandlerFactory;
import br.com.brainboss.evtx.handlers.XmlRootNodeHandler;
import br.com.brainboss.evtx.parser.ChunkHeader;
import br.com.brainboss.evtx.parser.FileHeader;
import br.com.brainboss.evtx.parser.FileHeaderFactory;
import br.com.brainboss.evtx.parser.MalformedChunkException;
import br.com.brainboss.evtx.parser.Record;
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
import java.util.logging.Level;
import org.apache.log4j.Logger;

public class EVTXPartitionReader implements PartitionReader<InternalRow> {

    private final EVTXInputPartition csvInputPartition;
    private final String fileName;
    private Iterator<String[]> iterator;
    private List<Function> valueConverters;
    private final FileHeaderFactory fileheaderfactory;
    private static final Logger log = Logger.getLogger(EVTXPartitionReader.class);
    private FileHeader fileheader;
    private ChunkHeader chunkheader;
    private final RootNodeHandlerFactory rootNodeHandlerFactory;

    public EVTXPartitionReader(
            EVTXInputPartition csvInputPartition,
            StructType schema,
            String fileName) throws IOException, URISyntaxException, MalformedChunkException {
        this.csvInputPartition = csvInputPartition;
        this.fileName = fileName;
        this.valueConverters = ValueConverters.getConverters(schema);
        this.fileheaderfactory = FileHeader::new;
        this.rootNodeHandlerFactory = XmlRootNodeHandler::new;
        this.createEvtxReader();
    }

    private void createEvtxReader() {
        try {
            log.debug("CreateEvtxReader joined");
            FileInputStream filereader;
            //URL resource = this.getClass().getClassLoader().getResource(this.fileName);
            log.debug("fileName"+this.fileName);
            filereader = new FileInputStream(new File(this.fileName));
            fileheader = fileheaderfactory.create(filereader, log);
            chunkheader = fileheader.next();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (MalformedChunkException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean next() {
        return fileheader.hasNext();
    }

    @Override
    public InternalRow get() {
        log.debug("EVTXPartitionReader::get joined");
        try {
            while (fileheader.hasNext()) {
                chunkheader = fileheader.next();
                while (chunkheader.hasNext()) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    RootNodeHandler rootNodeHandler = rootNodeHandlerFactory.create(out);
                    Record record = chunkheader.next();
                    rootNodeHandler.handle(record.getRootNode());
                    log.debug(out.toString());
                    out.close();
                }
            }
        } catch (MalformedChunkException | IOException e) {
            log.debug(String.valueOf(e));
            throw new RuntimeException(e);
        }


    // Object[] values = iterator.next();
    // Object[] convertedValues = new Object[values.length];
    // for (int i = 0; i < values.length; i++) {
    //     convertedValues[i] = valueConverters.get(i).apply(values[i]);
    // }
    // return InternalRow.apply(JavaConverters.asScalaIteratorConverter(Arrays.asList(convertedValues).iterator()).asScala().toSeq());
        return InternalRow.empty();
    }

    @Override
    public void close() throws IOException {

    }
}