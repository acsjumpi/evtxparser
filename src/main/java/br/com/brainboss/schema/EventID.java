package br.com.brainboss.schema;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlValue;

public class EventID {
    @XmlAttribute
    public int Qualifiers;
    @XmlValue
    public int text;
}
