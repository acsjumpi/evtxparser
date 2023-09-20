package br.com.brainboss.evtx.datasource;

import br.com.brainboss.evtx.handlers.RootNodeHandler;
import br.com.brainboss.evtx.handlers.RootNodeHandlerFactory;
import br.com.brainboss.evtx.handlers.XmlRootNodeHandler;
import br.com.brainboss.evtx.parser.ChunkHeader;
import br.com.brainboss.evtx.parser.FileHeader;
import br.com.brainboss.evtx.parser.FileHeaderFactory;
import br.com.brainboss.evtx.parser.MalformedChunkException;
import br.com.brainboss.evtx.parser.Record;
import br.com.brainboss.schema.Event;
import br.com.brainboss.schema.Events;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import scala.collection.JavaConverters;

import java.io.*;
import java.net.URISyntaxException;
import java.util.*;
import java.util.function.Function;

import org.apache.log4j.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

public class EVTXPartitionReader implements PartitionReader<InternalRow> {

    private final String fileName;
    private final FileHeaderFactory fileheaderfactory;
    private static final Logger log = Logger.getLogger(EVTXPartitionReader.class);
    private FileHeader fileheader;
    private ChunkHeader chunkheader;
    private final RootNodeHandlerFactory rootNodeHandlerFactory;
    private final EVTXInputPartition evtxInputPartition;
    private List<Function> valueConverters;

    public EVTXPartitionReader(
            EVTXInputPartition evtxInputPartition,
            StructType schema,
            String fileName) throws IOException, URISyntaxException, MalformedChunkException {
        this.evtxInputPartition = evtxInputPartition;
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
        if (chunkheader.hasNext())
            return true;
        if (fileheader.hasNext()) {
            try {
                chunkheader = fileheader.next();
            } catch (MalformedChunkException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            return false;
        }
        return chunkheader.hasNext();
    }

    @Override
    public InternalRow get() {
        log.debug("EVTXPartitionReader::get joined");
        Events xmlValue;
        InternalRow row;

        try {
            //while (fileheader.hasNext()) {
                //chunkheader = fileheader.next();
                //while (chunkheader.hasNext()) {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    //BufferedOutputStream out = new BufferedOutputStream(baos);
                    XmlRootNodeHandler rootNodeHandler = (XmlRootNodeHandler) rootNodeHandlerFactory.create(baos);
                    Record record = chunkheader.next();
                    rootNodeHandler.handle(record.getRootNode());
                    rootNodeHandler.close();
                    log.debug(baos.toString());

                    //XStream xs = new XStream(new StaxDriver());
                    //xs.registerConverter(new MapEntryConverter());
                    //xs.alias("Events", Map.class);
                    xmlValue = convertNodesFromXml(baos.toString());
                    log.debug(xmlValue);

                    Encoder<Events> eventsEncoder = Encoders.bean(Events.class);
                    ExpressionEncoder<Events> eventsExpressionEncoder = (ExpressionEncoder<Events>) eventsEncoder;
                    ExpressionEncoder.Serializer<Events> eventsSerializer = eventsExpressionEncoder.createSerializer();
                    row = eventsSerializer.apply(xmlValue);

               //}
            //}
        } catch (MalformedChunkException | IOException e) {
            log.debug(String.valueOf(e));
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        //return InternalRow.apply(JavaConverters.asScalaIteratorConverter(Arrays.asList(xmlValue).iterator()).asScala().toSeq());
        return row;
    }

    public static Events convertNodesFromXml(String xml) throws Exception {

        InputStream is = new ByteArrayInputStream(xml.getBytes());
        JAXBContext jaxbc = JAXBContext.newInstance(Events.class);
        Unmarshaller unmarshaller = jaxbc.createUnmarshaller();
        Events events = (Events) unmarshaller.unmarshal(is);

        return events;
    }

    @Override
    public void close() throws IOException {

    }
}