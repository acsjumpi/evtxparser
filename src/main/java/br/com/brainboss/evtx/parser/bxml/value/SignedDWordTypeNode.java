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

import com.google.common.primitives.UnsignedInteger;
import br.com.brainboss.evtx.parser.BinaryReader;
import br.com.brainboss.evtx.parser.ChunkHeader;
import br.com.brainboss.evtx.parser.bxml.BxmlNode;

import java.io.IOException;

/**
 * Node containing a signed 32 bit value
 */
public class SignedDWordTypeNode extends VariantTypeNode {
    private final UnsignedInteger value;

    public SignedDWordTypeNode(BinaryReader binaryReader, ChunkHeader chunkHeader, BxmlNode parent, int length) throws IOException {
        super(binaryReader, chunkHeader, parent, length);
        value = binaryReader.readDWord();
    }

    @Override
    public String getValue() {
        return Integer.toString(value.intValue());
    }
}
