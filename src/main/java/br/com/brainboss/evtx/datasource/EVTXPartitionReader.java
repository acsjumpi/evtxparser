package br.com.brainboss.evtx.datasource;

import br.com.brainboss.evtx.handlers.RootNodeHandlerFactory;
import br.com.brainboss.evtx.handlers.XmlRootNodeHandler;
import br.com.brainboss.evtx.parser.ChunkHeader;
import br.com.brainboss.evtx.parser.FileHeader;
import br.com.brainboss.evtx.parser.FileHeaderFactory;
import br.com.brainboss.evtx.parser.MalformedChunkException;
import br.com.brainboss.evtx.parser.Record;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import scala.collection.JavaConverters;

import java.io.*;
import java.net.URISyntaxException;
import java.util.*;
import java.util.function.Function;

import org.apache.log4j.Logger;

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

    private StructType schema;

    public EVTXPartitionReader(
            EVTXInputPartition evtxInputPartition,
            StructType schema,
            String fileName) throws IOException, URISyntaxException, MalformedChunkException {
        this.evtxInputPartition = evtxInputPartition;
        this.fileName = fileName;
        this.schema = schema;
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
            log.debug("fileName "+this.fileName);
            filereader = new FileInputStream(new File(this.fileName));
            fileheader = fileheaderfactory.create(filereader, log, true);
            chunkheader = fileheader.next();

            while(fileheader.hasNext() & chunkheader.getChunkNumber().compareTo(evtxInputPartition.getFirstChunk()) < 0)
                chunkheader = fileheader.next();

//            chunkheader = fileheader.next(evtxInputPartition.getFirstChunk().longValue());

            log.debug("chunkNumber"+chunkheader.getChunkNumber());
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        catch (MalformedChunkException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean next() {
//        if (chunkheader.hasNext())
//            return true;
//        if (fileheader.hasNext()) {
//            try {
//                chunkheader = fileheader.next();
//            } catch (MalformedChunkException e) {
//                throw new RuntimeException(e);
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        } else {
//            return false;
//        }
//        return chunkheader.hasNext();

        if(chunkheader.hasNext())
            return true;
        else {
            try {
                if(evtxInputPartition.isLast()) {
                    if (fileheader.hasNext()) {
                        chunkheader = fileheader.next();
                        return true;
                    }
                    return false;
                }
                if(chunkheader.getChunkNumber().compareTo(evtxInputPartition.getLastChunk()) >= 0)
                    return false;
                chunkheader = fileheader.next();
            } catch (MalformedChunkException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }


        return chunkheader.hasNext();
    }

    @Override
    public InternalRow get() {
        log.debug("EVTXPartitionReader::get joined");
        HashMap<String, Object> xmlMap;
        InternalRow row;

        try {
            //while (fileheader.hasNext()) {
                //chunkheader = fileheader.next();
                //while (chunkheader.hasNext()) {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    //BufferedOutputStream out = new BufferedOutputStream(baos);
                    XmlRootNodeHandler rootNodeHandler = (XmlRootNodeHandler) rootNodeHandlerFactory.create(baos);
                    log.debug("Chunk index: "+chunkheader.getChunkNumber());
                    Record record = chunkheader.next();
                    rootNodeHandler.handle(record.getRootNode());
                    rootNodeHandler.close();
                    log.debug(baos.toString());

                    //XStream xs = new XStream(new StaxDriver());
                    //xs.registerConverter(new MapEntryConverter());
                    //xs.alias("Events", Map.class);
                    xmlMap = (HashMap<String, Object>) convertNodesFromXml(baos.toString());
                    log.debug(xmlMap);

                    row = this.toInternalRow((HashMap<String, Object>) xmlMap.get("Event"), this.schema, this.valueConverters);
//                    this.iterateOverStruct(data, this.schema, this.valueConverters);

//                    xmlObject = this.toObjectArray((HashMap<String, Object>) xmlMap.get("Event"), this.schema, this.valueConverters);

//                    Encoder<Events> eventsEncoder = Encoders.bean(Events.class);
//                    ExpressionEncoder<Events> eventsExpressionEncoder = (ExpressionEncoder<Events>) eventsEncoder;
//                    ExpressionEncoder.Serializer<Events> eventsSerializer = eventsExpressionEncoder.createSerializer();
//                    row = eventsSerializer.apply(xmlValue);
               //}
            //}
        } catch (MalformedChunkException | IOException e) {
            //log.debug(String.valueOf(e));
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
//        return InternalRow.apply(JavaConverters.asScalaIteratorConverter(Arrays.asList(xmlObject).iterator()).asScala().toSeq());
        return row;
    }

    public static Object convertNodesFromXml(String xml) throws Exception {
        InputStream is = new ByteArrayInputStream(xml.getBytes());
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document document = db.parse(is);
        return createMap(document.getDocumentElement());
    }

    public static Object createMap(Node node) {
        Map<String, Object> map = new HashMap<String, Object>();
        NodeList nodeList = node.getChildNodes();

        if (nodeList.getLength() == 0)
            return null;

        for (int i = 0; i < nodeList.getLength(); i++) {
            Node currentNode = nodeList.item(i);
            String name = currentNode.getNodeName();
            Object value = null;

            if (currentNode.getNodeType() == Node.ELEMENT_NODE) {
                value = createMap(currentNode);

                if (currentNode.hasAttributes()) {
                    NamedNodeMap attrs = currentNode.getAttributes();
                    HashMap<String, Object> mapValues;

                    if (value instanceof String || value == null) {
                        mapValues = new HashMap<>();
                        mapValues.put(name, value);
                    } else {
                        mapValues = (HashMap<String, Object>)value;
                    }

                    for (int j = 0; j < attrs.getLength(); j++) {
                        Node attr = attrs.item(j);
                        String attrName = attr.getNodeName();
                        Object attrValue = attr.getNodeValue();

                        if (mapValues.containsKey(attrName)) {
                            Object os = mapValues.get(attrName);
                            if (os instanceof List) {
                                ((List<Object>)os).add(attrValue);
                            }
                            else {
                                List<Object> objs = new LinkedList<Object>();
                                objs.add(os);
                                objs.add(attrValue);
                                mapValues.put(attrName, objs);
                            }
                        }
                        else {
                            mapValues.put(attrName, attrValue);
                        }
                    }
                    value = mapValues;
                }
            }
            else if (currentNode.getNodeType() == Node.TEXT_NODE) {
                return currentNode.getTextContent();
            }

            if (map.containsKey(name)) {
                Object os = map.get(name);
                if (os instanceof List) {
                    ((List<Object>)os).add(value);
                }
                else {
                    List<Object> objs = new LinkedList<Object>();
                    objs.add(os);
                    objs.add(value);
                    map.put(name, objs);
                }
            }
            else {
                map.put(name, value);
            }
        }
        return map;
    }

    public InternalRow toInternalRow(HashMap<String, Object> data, StructType parentField, List<Function> parentValueConverters) {
        StructField[] fields = parentField.fields();
        Object[] parentConvertedValues = new Object[fields.length];

        for (int i = 0; i < fields.length; i++) {
            StructField field = fields[i];

            if(!data.containsKey(field.name())) {
                parentConvertedValues[i] = null;
                if (!field.nullable()){
                    throw new RuntimeException(field.name()+" Is not nullable.");
                }
            } else {
                DataType childField = field.dataType();

                if (childField instanceof StructType) {
                    log.debug("Node: " + field.name());
                    HashMap<String, Object> child = (HashMap<String, Object>) data.get(field.name());
                    List<Function> childValueConverters = (List<Function>) parentValueConverters.get(i).apply((StructType) childField);

                    parentConvertedValues[i] = toInternalRow(child, (StructType) childField, childValueConverters);
                } else {
                    log.debug("Leaf: " + field.name());
                    try {
                        parentConvertedValues[i] = parentValueConverters.get(i).apply((String) data.get(field.name()));
                    } catch (ClassCastException cce) {
                        log.debug("data.get: "+data.get(field.name()));
                        throw(cce);
                    }
                }
            }
        }

        return InternalRow.fromSeq(JavaConverters.asScalaIteratorConverter(Arrays.asList(parentConvertedValues).iterator()).asScala().toSeq());
    }

    @Override
    public void close() throws IOException {

    }
}