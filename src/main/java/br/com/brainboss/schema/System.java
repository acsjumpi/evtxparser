package br.com.brainboss.schema;

import javax.xml.bind.annotation.XmlElement;

public class System {
    @XmlElement
    public Provider Provider;
    @XmlElement
    public EventID EventID;
    @XmlElement
    public int Version;
    @XmlElement
    public int Level;
    @XmlElement
    public int Task;
    @XmlElement
    public int Opcode;
    @XmlElement
    public String Keywords;
    @XmlElement
    public TimeCreated TimeCreated;
    @XmlElement
    public int EventRecordID;
    @XmlElement
    public Correlation Correlation;
    @XmlElement
    public Execution Execution;
    @XmlElement
    public String Channel;
    @XmlElement
    public String Computer;
    @XmlElement
    public Security Security;
}