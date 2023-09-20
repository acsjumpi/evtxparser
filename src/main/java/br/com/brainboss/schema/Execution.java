package br.com.brainboss.schema;

import javax.xml.bind.annotation.XmlAttribute;

public class Execution {
    @XmlAttribute
    public int ProcessID;
    @XmlAttribute
    public int ThreadID;
}