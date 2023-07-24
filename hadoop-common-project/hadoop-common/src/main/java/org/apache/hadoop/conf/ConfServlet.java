package org.apache.hadoop.conf;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.http.HttpServer2;
import org.apache.http.HttpHeaders;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/22
 **/
@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
@InterfaceStability.Unstable
public class ConfServlet extends HttpServlet {
    private static final long serialVersionUID=1L;
    protected static final String FORMAT_JSON="json";
    protected static final String FORMAT_XML="xml";
    private Configuration getConfFromContext(){
        Configuration conf=(Configuration)getServletContext().getAttribute(HttpServer2.CONF_CONTEXT_ATTRIBUTE);
        assert conf!=null;
        return conf;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        if (!HttpServer2.isInstrumentationAccessAllowed(getServletContext(),req,resp)){
            return;
        }
        String format=parseAcceptHeader(req);
        if (FORMAT_XML.equals(format)) {
            resp.setContentType("text/html; charset=utf-8");
        } else if (FORMAT_JSON.equals(format)) {
            resp.setContentType("application/json; charset=utf-8");
        }
        String name = req.getParameter("name");
        Writer out = resp.getWriter();
        try {
            writeResponse(getConfFromContext(),out,format,name);
        } catch (BadFormatException e) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST,e.getMessage());
        }catch (IllegalArgumentException e){
            resp.sendError(HttpServletResponse.SC_NOT_FOUND,e.getMessage());
        }
        out.close();
    }

    static void writeResponse(Configuration confFromContext, Writer out, String format, String name) throws BadFormatException, IOException {
        if (FORMAT_JSON.equals(format)) {
            Configuration.dumpConfiguration(confFromContext,name,out);
        } else if (FORMAT_XML.equals(format)) {
            confFromContext.writeXml(name,out,confFromContext);
        }else {
            throw new BadFormatException("Bad format: "+format);
        }
    }
    static void writeResponse(Configuration confFromContext, Writer out, String format) throws IOException, BadFormatException {
        writeResponse(confFromContext,out,format,null);
    }

    @VisibleForTesting
    static String parseAcceptHeader(HttpServletRequest request){
        String format = request.getHeader(HttpHeaders.ACCEPT);
        return format!=null&&format.contains(FORMAT_JSON)?FORMAT_JSON:FORMAT_XML;
    }

    public static class BadFormatException extends Exception{
        private static final long serialVersionUID=1L;
        public BadFormatException(String msg){
            super(msg);
        }
    }
}
