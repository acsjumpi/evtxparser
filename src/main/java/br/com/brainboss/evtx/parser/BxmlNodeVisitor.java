package br.com.brainboss.evtx.parser;

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

import br.com.brainboss.evtx.parser.bxml.AttributeNode;
import br.com.brainboss.evtx.parser.bxml.CDataSectionNode;
import br.com.brainboss.evtx.parser.bxml.CloseElementNode;
import br.com.brainboss.evtx.parser.bxml.CloseEmptyElementNode;
import br.com.brainboss.evtx.parser.bxml.CloseStartElementNode;
import br.com.brainboss.evtx.parser.bxml.ConditionalSubstitutionNode;
import br.com.brainboss.evtx.parser.bxml.EndOfStreamNode;
import br.com.brainboss.evtx.parser.bxml.EntityReferenceNode;
import br.com.brainboss.evtx.parser.bxml.NameStringNode;
import br.com.brainboss.evtx.parser.bxml.NormalSubstitutionNode;
import br.com.brainboss.evtx.parser.bxml.OpenStartElementNode;
import br.com.brainboss.evtx.parser.bxml.ProcessingInstructionDataNode;
import br.com.brainboss.evtx.parser.bxml.ProcessingInstructionTargetNode;
import br.com.brainboss.evtx.parser.bxml.RootNode;
import br.com.brainboss.evtx.parser.bxml.StreamStartNode;
import br.com.brainboss.evtx.parser.bxml.TemplateInstanceNode;
import br.com.brainboss.evtx.parser.bxml.TemplateNode;
import br.com.brainboss.evtx.parser.bxml.ValueNode;
import br.com.brainboss.evtx.parser.bxml.value.VariantTypeNode;

import java.io.IOException;

/**
 * Visitor interface for traversing a RootNode
 */
public interface BxmlNodeVisitor {
    default void visit(RootNode rootNode) throws IOException {

    }

    default void visit(TemplateInstanceNode templateInstanceNode) throws IOException {

    }

    default void visit(TemplateNode templateNode) throws IOException {

    }

    default void visit(ValueNode valueNode) throws IOException {

    }

    default void visit(StreamStartNode streamStartNode) throws IOException {

    }

    default void visit(ProcessingInstructionTargetNode processingInstructionTargetNode) throws IOException {

    }

    default void visit(ProcessingInstructionDataNode processingInstructionDataNode) throws IOException {

    }

    default void visit(OpenStartElementNode openStartElementNode) throws IOException {

    }

    default void visit(NormalSubstitutionNode normalSubstitutionNode) throws IOException {

    }

    default void visit(NameStringNode nameStringNode) throws IOException {

    }

    default void visit(EntityReferenceNode entityReferenceNode) throws IOException {

    }

    default void visit(EndOfStreamNode endOfStreamNode) throws IOException {

    }

    default void visit(ConditionalSubstitutionNode conditionalSubstitutionNode) throws IOException {

    }

    default void visit(CloseStartElementNode closeStartElementNode) throws IOException {

    }

    default void visit(CloseEmptyElementNode closeEmptyElementNode) throws IOException {

    }

    default void visit(CloseElementNode closeElementNode) throws IOException {

    }

    default void visit(CDataSectionNode cDataSectionNode) throws IOException {

    }

    default void visit(AttributeNode attributeNode) throws IOException {

    }

    default void visit(VariantTypeNode variantTypeNode) throws IOException {

    }
}
