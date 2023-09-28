package br.com.brainboss.evtx.parser.bxml.value;

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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import br.com.brainboss.evtx.datasource.EVTXPartitionReader;
import br.com.brainboss.evtx.parser.BinaryReader;
import br.com.brainboss.evtx.parser.ChunkHeader;
import br.com.brainboss.evtx.parser.bxml.BxmlNode;
import org.apache.log4j.Logger;

/**
 * Node representing an array of wstring values
 */
public class WStringArrayTypeNode extends VariantTypeNode {
    public static final XMLOutputFactory XML_OUTPUT_FACTORY = XMLOutputFactory.newFactory();
    private final String value;
    private static final Logger log = Logger.getLogger(WStringArrayTypeNode.class);

    public WStringArrayTypeNode(BinaryReader binaryReader, ChunkHeader chunkHeader, BxmlNode parent, int length) throws IOException {
        super(binaryReader, chunkHeader, parent, length);
        String raw;
        if (length >= 0) {
            raw = binaryReader.readWString(length / 2);
        } else {
            int binaryLength = binaryReader.readWord();
            raw = binaryReader.readWString(binaryLength / 2);
        }
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        List<String> strings = new ArrayList<>();
        try {
            if (raw.equals("\u0000")) {
                value = "";
                return;
            }
            for (String s : raw.split("\u0000")) {
                XMLStreamWriter xmlStreamWriter = XML_OUTPUT_FACTORY.createXMLStreamWriter(stream, "UTF-8");
                log.debug("s value: "+s);
                xmlStreamWriter.writeStartElement("string");
                try {
                    xmlStreamWriter.writeCharacters(s);
                } finally {
                    xmlStreamWriter.writeEndElement();
                    xmlStreamWriter.close();
                    strings.add(stream.toString("UTF-8"));
                    stream.reset();
                }
            }
        } catch (XMLStreamException e) {
            log.error("StrArray content: "+raw);
            log.error("Length size: "+length);
            throw new IOException(e);
        }
        value = strings.stream().collect(Collectors.joining());
    }

    @Override
    public String getValue() {
        return value;
    }
}
