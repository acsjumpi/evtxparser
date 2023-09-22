package br.com.brainboss.evtx.handlers;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import br.com.brainboss.evtx.parser.bxml.RootNode;
import org.apache.log4j.Logger;

public class XmlRootNodeHandler implements RootNodeHandler {
    public static final String EVENTS = "Events";
    private static final XMLOutputFactory XML_OUTPUT_FACTORY = XMLOutputFactory.newFactory();

    private final XMLStreamWriter xmlStreamWriter;
    private final XmlBxmlNodeVisitorFactory xmlBxmlNodeVisitorFactory;
    private static final Logger log = Logger.getLogger(XmlRootNodeHandler.class);

    public XmlRootNodeHandler(OutputStream outputStream) throws IOException {
        this.xmlStreamWriter = getXmlStreamWriter(new BufferedOutputStream(outputStream));
        this.xmlBxmlNodeVisitorFactory = XmlBxmlNodeVisitor::new;
        start();
        log.debug("XmlRootNodeHandler Constructor joined");
    }

    public void start () throws IOException {
        try {
            this.xmlStreamWriter.writeStartDocument();
            try {
                this.xmlStreamWriter.writeStartElement(EVENTS);
            } catch (XMLStreamException e) {
                this.xmlStreamWriter.close();
                throw e;
            }
        } catch (XMLStreamException e) {
            throw new IOException(e);
        }
    }

    private static XMLStreamWriter getXmlStreamWriter(OutputStream outputStream) throws IOException {
        log.debug("getXmlStreamWriter joined");
        try {
            XMLStreamWriter xmlOutput = XML_OUTPUT_FACTORY.createXMLStreamWriter(outputStream, "UTF-8");
            log.debug("outputStream"+outputStream.toString());
            return xmlOutput;
            //return XML_OUTPUT_FACTORY.createXMLStreamWriter(outputStream, "UTF-8");
        } catch (XMLStreamException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void handle(RootNode rootNode) throws IOException {
        xmlBxmlNodeVisitorFactory.create(xmlStreamWriter, rootNode);
    }

    @Override
    public void close() throws IOException {
        log.debug("XmlRootNodeHandler::close joined");
        try {
            xmlStreamWriter.writeEndElement();
        } catch (XMLStreamException e) {
            throw new IOException(e);
        } finally {
            try {
                xmlStreamWriter.close();
            } catch (XMLStreamException e) {
                throw new IOException(e);
            }
        }
    }
}
