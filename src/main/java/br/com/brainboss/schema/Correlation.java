package br.com.brainboss.schema;

import javax.xml.bind.annotation.XmlAttribute;

public class Correlation {
    @XmlAttribute
    public Object ActivityID;
    @XmlAttribute
    public Object RelatedActivityID;
}