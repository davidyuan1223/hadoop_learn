package org.apache.hadoop.conf;

import com.sun.xml.internal.ws.api.config.management.Reconfigurable;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Enumeration;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/22
 **/
public class ReconfigurationServlet extends HttpServlet {
    private static final long serialVersionUID=1L;
    private static final Logger LOG= LoggerFactory.getLogger(ReconfigurationServlet.class);
    public static final String CONF_SERVLET_RECONFIGURABLE_PREFIX="conf.servlet.reconfigurable";

    @Override
    public void init() throws ServletException {
        super.init();
    }
    private ReConfigurable getReconfigurable(HttpServletRequest request){
        LOG.info("servlet path: "+request.getServletPath());
        LOG.info("getting attribute: "+CONF_SERVLET_RECONFIGURABLE_PREFIX+request.getServletPath());
        return (ReConfigurable)this.getServletContext()
                .getAttribute(CONF_SERVLET_RECONFIGURABLE_PREFIX+request.getServletPath());
    }
    private void printHeader(PrintWriter out,String nodeName){
        out.print("<html><head>");
        out.printf("<title>%s Reconfiguration Utility</title>%n", StringEscapeUtils.escapeHtml4(nodeName));
        out.print("</head><body>\n");
        out.printf("<h1>%s Reconfiguration Utility</h1>%n",
                StringEscapeUtils.escapeHtml4(nodeName));
    }
    private void printFooter(PrintWriter out){
        out.print("</body></html>\n");
    }
    private void printConf(PrintWriter out,ReConfigurable reconf){
        Configuration oldCOnf = reconf.getConf();
        Configuration newConf=new Configuration();
        Collection<ReconfigurationUtil.PropertyChange> changes = ReconfigurationUtil.getChangedProperties(newConf, oldCOnf);
        boolean changeOK=true;
        out.println("<form action=\"\" method=\"post\">");
        out.println("<table border=\"1\">");
        out.println("<tr><th>Property</th><th>Old value</th>");
        out.println("<th>New value </th><th></th></tr>");
        for (ReconfigurationUtil.PropertyChange c : changes) {
            out.println("<tr><td>");
            if (!reconf.isPropertyReconfigurable(c.prop)) {
                out.println("<font color=\"red\">" +
                        StringEscapeUtils.escapeHtml4(c.prop)+"</font>");
                changeOK=false;
            }else {
                out.print(StringEscapeUtils.escapeHtml4(c.prop));
                out.print("<input type=\"hidden\" name=\"" +
                        StringEscapeUtils.escapeHtml4(c.prop)+"\" value=\"" +
                        StringEscapeUtils.escapeHtml4(c.newVal)+"\"/>");
            }
            out.print("</td><td>" +
                    (c.oldVal==null?"<it>default</it>":StringEscapeUtils.escapeHtml4(c.oldVal))+
                    "</td><td>"+
                    (c.newVal==null?"<it>default</it>":StringEscapeUtils.escapeHtml4(c.newVal))+
                    "</td>");
            out.print("</tr>\n");
        }
        out.println("</table>");
        if (!changeOK) {
            out.println("<p><font color=\"red\">WARNING: properties marked red" +
                    " will not be changed until the next restart.</font></p>");
        }
        out.println("<input type=\"submit\" value=\"Apply\" />");
        out.println("</form>");
    }
    @SuppressWarnings("unchecked")
    private Enumeration<String > getParams(HttpServletRequest request){
        return request.getParameterNames();
    }
    private void applyChanges(PrintWriter out, ReConfigurable reconf,
                              HttpServletRequest request) throws ReconfigurationException {
        Configuration oldConf = reconf.getConf();
        Configuration newConf=new Configuration();
        Enumeration<String > params=getParams(request);
        synchronized (oldConf){
            while (params.hasMoreElements()) {
                String rawParam = params.nextElement();
                String param = StringEscapeUtils.unescapeHtml4(rawParam);
                String value = StringEscapeUtils.unescapeHtml4(request.getParameter(rawParam));
                if (value != null) {
                    if (value.equals(newConf.getRaw(param))
                            || value.equals("default")
                            || value.equals("null")
                            || value.isEmpty()) {
                        if (value.equals("default") || value.equals("null")
                                || value.isEmpty() && oldConf.getRaw(param) != null) {
                            out.println("<p>Changed \"" +
                                    StringEscapeUtils.escapeHtml4(param)+"\" from \"" +
                                    StringEscapeUtils.escapeHtml4(oldConf.getRaw(param))+
                                    "\" to default</p>");
                            reconf.reconfigureProperty(param,null);
                        }else if (!value.equals("default") && !value.equals("null")
                        &&!value.isEmpty()&&(oldConf.getRaw(param)==null||!oldConf.getRaw(param).equals(value))){
                            if (oldConf.getRaw(param) == null) {
                                out.println("<p>Changed \"" +
                                        StringEscapeUtils.escapeHtml4(param) +
                                        "\" from default to \"" +
                                        StringEscapeUtils.escapeHtml4(value) + "\"</p>");
                            }else {
                                out.println("<p>Changed \"" +
                                        StringEscapeUtils.escapeHtml4(param) + "\" from \"" +
                                        StringEscapeUtils.escapeHtml4(oldConf.
                                                getRaw(param)) +
                                        "\" to \"" +
                                        StringEscapeUtils.escapeHtml4(value) + "\"</p>");
                            }
                            reconf.reconfigureProperty(param,value);
                        }else {
                            LOG.info("property "+param+" unchanged");
                        }
                    }else {
                        out.println("<p>\"" + StringEscapeUtils.escapeHtml4(param) +
                                "\" not changed because value has changed from \"" +
                                StringEscapeUtils.escapeHtml4(value) + "\" to \"" +
                                StringEscapeUtils.escapeHtml4(newConf.getRaw(param)) +
                                "\" since approval</p>");
                    }
                }
            }
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        LOG.info("GET");
        resp.setContentType("text/html");
        PrintWriter out = resp.getWriter();
        ReConfigurable reconf = getReconfigurable(req);
        String nodeName = reconf.getClass().getCanonicalName();
        printHeader(out,nodeName);
        printConf(out,reconf);
        printFooter(out);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        LOG.info("POST");
        resp.setContentType("text/html");
        PrintWriter out = resp.getWriter();
        ReConfigurable reconf = getReconfigurable(req);
        String nodeName = reconf.getClass().getCanonicalName();
        printHeader(out,nodeName);
        try {
            applyChanges(out,reconf,req);
        }catch (ReconfigurationException e){
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    StringUtils.stringifyException(e));
            return;
        }
        out.println("<p><a href=\"" + req.getServletPath() + "\">back</a></p>");
        printFooter(out);
    }
}