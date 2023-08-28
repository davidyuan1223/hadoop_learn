package org.apache.hadoop.security;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class UserGroupInformation {
    @VisibleForTesting
    static final Logger LOG= LoggerFactory.getLogger(UserGroupInformation.class);
    private static final float TICKET_RENEW_WINDOW=0.08f;
    private static boolean shouldRenewImmediatelyForTests=false;
    static final String HADOOP_USER_NAME="HADOOP_USER_NAME";
    static final String HADOOP_PROXY_USER="HADOOP_PROXY_USER";
    @VisibleForTesting
    public static void setShouldRenewImmediatelyForTests(boolean immediatelyForTests){
        shouldRenewImmediatelyForTests=immediatelyForTests;
    }

    @Metrics(about = "User and group related metrics",context = "ugi")
    static class UgiMetrics{
        final MetricsRegistry registry=new MetricsRegistry("UgiMetrics");
        @Metric("Rated of successful kerberos logins and latency (milliseconds)")
        MutableRate loginSuccess;
        @Metric("Rated of failed kerberos logins and latency (milliseconds)")
        MutableRate loginFailure;
        @Metric("GetGroups")
        MutableRate getGroups;
        MutableQuantiles[] getGroupsQuantiles;
    }
    public static boolean isInitialized() {
    }

    public static UserGroupInformation getCurrentUser()throws IOException {
    }

    public Object getRealUser() {

    }
}
