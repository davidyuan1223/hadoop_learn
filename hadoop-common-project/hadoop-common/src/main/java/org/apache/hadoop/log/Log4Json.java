package org.apache.hadoop.log;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ContainerNode;
import org.apache.log4j.Layout;
import org.apache.log4j.helpers.ISO8601DateFormat;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.text.DateFormat;
import java.util.Date;

public class Log4Json extends Layout {
    private static final JsonFactory factory=new MappingJsonFactory();
    public static final ObjectReader READER=new ObjectMapper(factory).reader();
    public static final String DATE="date";
    public static final String EXCEPTION_CLASS="exceptionclass";
    public static final String LEVEL="level";
    public static final String MESSAGE="message";
    public static final String NAME="name";
    public static final String STACK="stack";
    public static final String THREAD="thread";
    public static final String TIME="time";
    public static final String JSON_TYPE="application/json";
    private final DateFormat dateFormat;

    public Log4Json(){
        dateFormat=new ISO8601DateFormat();
    }
    @Override
    public String getContentType() {
        return JSON_TYPE;
    }

    @Override
    public String format(LoggingEvent event) {
        try {
            return toJson(event);
        }catch (IOException e){
            return "{ \"logfailure\":\""+e.getClass().toGenericString()+"\"}";
        }
    }
    public String toJson(LoggingEvent event)throws IOException{
        StringWriter writer = new StringWriter();
        toJson(writer,event);
        return writer.toString();
    }
    public Writer toJson(final Writer writer,final LoggingEvent event)throws IOException{
        ThrowableInformation ti = event.getThrowableInformation();
        toJson(writer,
                event.getLoggerName(),
                event.getTimeStamp(),
                event.getLevel().toString(),
                event.getThreadName(),
                event.getRenderedMessage(),
                ti);
        return writer;
    }
    public Writer toJson(final Writer writer,
                         final String loggerName,
                         final long timestamp,
                         final String level,
                         final String threadName,
                         final String message,
                         final ThrowableInformation ti)throws IOException{
        JsonGenerator json = factory.createGenerator(writer);
        json.writeStartObject();
        json.writeStringField(NAME,loggerName);
        json.writeNumberField(TIME,timestamp);
        Date date = new Date(timestamp);
        json.writeStringField(DATE,dateFormat.format(date));
        json.writeStringField(LEVEL,level);
        json.writeStringField(THREAD,threadName);
        json.writeStringField(MESSAGE,message);
        if (ti != null) {
            Throwable thrown = ti.getThrowable();
            String eclass=(thrown!=null)?thrown.getClass().getName():"";
            json.writeStringField(EXCEPTION_CLASS,eclass);
            String[] stackTrace = ti.getThrowableStrRep();
            json.writeArrayFieldStart(STACK);
            for (String row : stackTrace) {
                json.writeString(row);
            }
            json.writeEndArray();
        }
        json.writeEndObject();
        json.flush();
        json.close();
        return writer;
    }


    @Override
    public boolean ignoresThrowable() {
        return false;
    }

    @Override
    public void activateOptions() {

    }

    public static ContainerNode parse(String json)throws IOException{
        JsonNode jsonNode = READER.readTree(json);
        if (!(jsonNode instanceof ContainerNode)) {
            throw new IOException("Wrong JSON data: "+json);
        }
        return (ContainerNode) jsonNode;
    }

}
