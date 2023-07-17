package org.apache.hadoop.security.authentication.client;

import java.io.IOException;
import java.net.HttpURLConnection;

/**
 * @Description: Interface for configure HttpURLConnection created by AuthenticatedURL instances
 * @Author: yuan
 * @Date: 2023/07/16
 **/
public interface ConnectionConfigurator {
    /**
     * Configures the given  HttpURLConnection instance
     * @param connection the HttpURLConnection instance to configure
     * @return the configured HttpURLConnection instance
     * @throws IOException if an IO error occurred
     */
    public HttpURLConnection configure(HttpURLConnection connection)throws IOException;
}
