package br.com.brainboss.schema;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlValue;

public class Event {
    @XmlElement
    public System System;
    @XmlElement
    public EventData EventData;
    @XmlAttribute
    public String xmlns;
}