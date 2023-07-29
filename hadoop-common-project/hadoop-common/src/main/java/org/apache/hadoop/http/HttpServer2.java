package org.apache.hadoop.http;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class HttpServer2 implements FilterContainer{
    public static final Logger LOG= LoggerFactory.getLogger(HttpServer2.class);
    public static final String HTTP_SCHEME="http";
    public static final String HTTPS_SCHEME="https";
    public static final String HTTP_MAX_REQUEST_HEADER_SIZE_KEY="hadoop.http.max.request.header.size";
    public static final int HTTP_MAX_REQUEST_HEADER_SIZE_DEFAULT=65536;
    public static final String HTTP_MAX_RESPONSE_HEADER_SIZE_KEY="hadoop.http.max.response.header.size";
    public static final int HTTP_MAX_RESPONSE_HEADER_SIZE_DEFAULT=65536;

    public static final String HTTP_SOCKET_BACKLOG_SIZE_KEY="hadoop.http.socket.backlog.size";
    public static final int HTTP_SOCKET_BACKLOG_SIZE_DEFAULT=500;
    public static final String HTTP_MAX_THREADS_KEY="hadoop.http.max.threads";
    public static final String HTTP_ACCEPTOR_COUNT_KEY="hadoop.http.acceptor.count";
    public static final int HTTP_ACCEPTOR_COUNT_DEFAULT=-1;
    public static final String HTTP_SELECTOR_COUNT_KEY="hadoop.http.selector.count";
    public static final int HTTP_SELECTOR_COUNT_DEFAULT=-1;
    public static final String HTTP_IDLE_TIMEOUT_MS_KEY="hadoop.http.idle_timeout.ms";
    public static final int HTTP_IDLE_TIMEOUT_MS_DEFAULT=60000;
    public static final String HTTP_TEMP_DIR_KEY="hadoop.http.temp.dir";

    public static final String FILTER_INITIALIZER_PROPERTY="hadoop.http.filter.initializers";

    public static final String HTTP_SNI_HOST_CHECK_ENABLED_KEY="hadoop.http.sni.host.check.enabled";
    public static final boolean HTTP_SNI_HOST_CHECK_ENABLED_DEFAULT=false;

    public static final String CONF_CONTEXT_ATTRIBUTE="hadoop.conf";
    public static final String ADMINS_ACL="admins.acl";
    public static final String SPNEGO_FILTER="authentication";
    public static final String NO_CACHE_FILTER="NoCacheFilter";

    public static final String BIND_ADDRESS="bin.address";


}
