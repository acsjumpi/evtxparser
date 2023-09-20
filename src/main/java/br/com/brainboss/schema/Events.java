package br.com.brainboss.schema;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Events {
    @XmlElement
    public Event Event;
}
