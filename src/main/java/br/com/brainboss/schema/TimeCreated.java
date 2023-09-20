package br.com.brainboss.schema;

import javax.xml.bind.annotation.XmlAttribute;
import java.util.Date;

public class TimeCreated {
    @XmlAttribute
    public Date SystemTime;
}
