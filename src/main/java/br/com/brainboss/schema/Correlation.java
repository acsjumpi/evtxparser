package br.com.brainboss.schema;

import javax.xml.bind.annotation.XmlAttribute;

public class Correlation {
    @XmlAttribute
    public String ActivityID;
    @XmlAttribute
    public String RelatedActivityID;
}