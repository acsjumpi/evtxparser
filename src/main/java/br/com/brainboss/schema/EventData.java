package br.com.brainboss.schema;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;

public class EventData {
    @XmlElement
    public String Data;
    @XmlElement
    public Object Binary;
}
