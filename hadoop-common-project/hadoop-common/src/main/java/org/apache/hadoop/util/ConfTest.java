package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.File;
import java.io.FileFilter;
import java.io.InputStream;
import java.util.*;

@InterfaceAudience.Private
public final class ConfTest {
    private static final String USAGE=
            "Usage: hadoop conftest [-conffile <path>|-h|--help]\n"
            +"  Options:\n"
            +"  \n"
            +"  -conffile <path>\n"
            +"   If not specified, the files in ${HADOOP_CONF_CIF}\n"
            +"   whose name end with .xml will be verified.\n"
            +"   If specified, that path will be verified.\n"
            +"   You can specify either a file or directory, and\n"
            +"   if a directory specified, the files in that directory\n"
            +"   whose name end with .xml will be verified.\n"
            +"   You can specify this option multiple times.\n"
            +"  -h,--help             Print this help";
    private static final String HADOOP_CONF_DIR="HADOOP_CONF_DIR";
    protected ConfTest(){
        super();
    }
    private static List<NodeInfo> parseConf(InputStream in) throws XMLStreamException {
        QName configuration = new QName("configuration");
        QName property = new QName("property");
        List<NodeInfo> nodes=new ArrayList<>();
        Stack<NodeInfo> parsed=new Stack<>();
        XMLInputFactory factory = XMLInputFactory.newInstance();
        XMLEventReader reader = factory.createXMLEventReader(in);
        while (reader.hasNext()) {
            XMLEvent event = reader.nextEvent();
            if (event.isStartElement()) {
                StartElement currentElement = event.asStartElement();
                NodeInfo currentNode = new NodeInfo(currentElement);
                if (parsed.isEmpty()) {
                    if (!currentElement.getName().equals(configuration)) {
                        return null;
                    }
                }else {
                    NodeInfo parentNode = parsed.peek();
                    QName parentName = parentNode.getStartElement().getName();
                    if (parentName.equals(configuration)
                    &&currentNode.getStartElement().getName().equals(property)) {
                        @SuppressWarnings("unchecked")
                        Iterator<Attribute> it = currentElement.getAttributes();
                        while (it.hasNext()) {
                            currentNode.addAttribute(it.next());
                        }
                    } else if (parentName.equals(property)) {
                        parentNode.addElement(currentElement);
                    }
                }
                parsed.push(currentNode);
            }else if (event.isEndElement()) {
                NodeInfo node = parsed.pop();
                if (parsed.size() == 1) {
                    nodes.add(node);
                }
            }else if (event.isCharacters()) {
                if (2<parsed.size()) {
                    NodeInfo parentNode = parsed.pop();
                    StartElement parentElement = parentNode.getStartElement();
                    NodeInfo grahpparentNode = parsed.peek();
                    if (grahpparentNode.getElement(parentElement) == null) {
                        grahpparentNode.setElement(parentElement,event.asCharacters());
                    }
                    parsed.push(parentNode);
                }
            }
        }
        return nodes;
    }
    public static List<String > checkConf(InputStream in){
        List<NodeInfo> nodes=null;
        List<String > errors=new ArrayList<String>();
        try {
            nodes=parseConf(in);
            if (nodes == null) {
                errors.add("bad conf file: top-level element not <configuration>");
            }
        }catch (XMLStreamException e) {
            errors.add("bad conf file: "+e.getMessage());
        }
        if (!errors.isEmpty()) {
            return errors;
        }
        Map<String ,List<Integer>> duplicatedProperties=new HashMap<>();
        for (NodeInfo node : nodes) {
            StartElement element = node.getStartElement();
            int line = element.getLocation().getLineNumber();
            if (!element.getName().equals(new QName("property"))) {
                errors.add(String.format("Line %d: element not <property>",line));
                continue;
            }
            List<XMLEvent> events = node.getXMLEventsForQName(new QName("name"));
            if (events == null) {
                errors.add(String.format("Line %d: <property> has no <name>",line));
            }else {
                String v=null;
                for (XMLEvent event : events) {
                    if (event.isAttribute()) {
                        v=((Attribute)event).getValue();
                    }else {
                        Characters c = node.getElement(event.asStartElement());
                        if (c != null) {
                            v=c.getData();
                        }
                    }
                    if (v == null || v.isEmpty()) {
                        errors.add(String.format("Line %d: <property> has an empty <name>",line));
                    }
                }
                if (v != null && !v.isEmpty()) {
                    List<Integer> lines = duplicatedProperties.get(v);
                    if (lines == null) {
                        lines=new ArrayList<>();
                        duplicatedProperties.put(v,lines);
                    }
                    lines.add(node.getStartElement().getLocation().getLineNumber());
                }
            }
            events = node.getXMLEventsForQName(new QName("value"));
            if (events == null) {
                errors.add(String.format("Line %d: <property> has no <value>",line));
            }
            for (QName qName : node.getDuplicatedQNames()) {
                if (!qName.equals(new QName("source"))) {
                    errors.add(String.format("Line %d: <property> has duplicated <%s>s",line,qName));
                }
            }
        }
        for (Map.Entry<String, List<Integer>> e : duplicatedProperties.entrySet()) {
            List<Integer> lines = e.getValue();
            if (1 < lines.size()) {
                errors.add(String.format("Line %s: duplicated <property>s for %s",StringUtils.join(", ",lines),e.getKey()));
            }
        }
        return errors;
    }

    private static File[] listFiles(File dir){
        return dir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.isFile()&&file.getName().endsWith(".xml");
            }
        });
    }
    @SuppressWarnings("static-access")
    public static void main(String[] args) {
        new GenericOptionParser()
    }


    static class NodeInfo{
        private StartElement startElement;
        private List<Attribute> attributes=new ArrayList<>();
        private Map<StartElement, Characters> elements=new HashMap<>();
        private Map<QName,List<XMLEvent>> qNameXMLEventsMap=new HashMap<>();
        public NodeInfo(StartElement startElement){
            this.startElement=startElement;
        }
        private void addQNameXMLEvent(QName qName, XMLEvent event){
            List<XMLEvent> events = qNameXMLEventsMap.get(qName);
            if (events == null) {
                events=new ArrayList<>();
                qNameXMLEventsMap.put(qName, events);
            }
            events.add(event);
        }

        public StartElement getStartElement() {
            return startElement;
        }
        public void addAttribute(Attribute attribute){
            attributes.add(attribute);
            addQNameXMLEvent(attribute.getName(),attribute);
        }
        public Characters getElement(StartElement startElement) {
            return elements.get(startElement);
        }
        public void addElement(StartElement element){
            setElement(element,null);
            addQNameXMLEvent(element.getName(),element);
        }
        public void setElement(StartElement element,Characters characters){
            elements.put(element,characters);
        }

        public List<QName> getDuplicatedQNames(){
            List<QName> duplicatedQNames=new ArrayList<>();
            for (Map.Entry<QName,List<XMLEvent>> entry:qNameXMLEventsMap.entrySet()) {
                if (entry.getValue().size() > 1) {
                    duplicatedQNames.add(entry.getKey());
                }
            }
            return duplicatedQNames;
        }
        public List<XMLEvent> getXMLEventsForQName(QName qName) {
            return qNameXMLEventsMap.get(qName);
        }
    }
}
