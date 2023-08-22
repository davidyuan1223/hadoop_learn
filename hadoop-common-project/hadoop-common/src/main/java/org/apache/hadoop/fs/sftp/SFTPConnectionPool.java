package org.apache.hadoop.fs.sftp;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

class SFTPConnectionPool {
    public static final Logger LOG= LoggerFactory.getLogger(SFTPFileSystem.class);
    private int maxConnection;
    private int liveConnectionCount=0;
    private HashMap<ConnectionInfo, HashSet<ChannelSftp>> idleConnection=new HashMap<>();
    private HashMap<ChannelSftp,ConnectionInfo> con2infoMap=new HashMap<>();

    SFTPConnectionPool(int maxConnection){
        this.maxConnection=maxConnection;
    }
    synchronized ChannelSftp getFromPool(ConnectionInfo info)throws IOException {
        HashSet<ChannelSftp> cons = idleConnection.get(info);
        ChannelSftp channel;
        if (cons != null && cons.size() > 0) {
            Iterator<ChannelSftp> it = cons.iterator();
            if (it.hasNext()) {
                channel=it.next();
                idleConnection.remove(info);
                return channel;
            }else {
                throw new IOException("Connection pool error.");
            }
        }
        return null;
    }
    synchronized void returnToPool(ChannelSftp channel){
        ConnectionInfo info = con2infoMap.get(channel);
        HashSet<ChannelSftp> cons = idleConnection.get(info);
        if (cons == null) {
            cons=new HashSet<>();
            idleConnection.put(info,cons);
        }
        cons.add(channel);
    }

    synchronized void shutdown(){
        if (this.con2infoMap == null) {
            return;
        }
        LOG.info("Inside shutdown, con2infoMap size="+con2infoMap.size());
        this.maxConnection=0;
        Set<ChannelSftp> cons = con2infoMap.keySet();
        if (cons != null && cons.size() > 0) {
            Set<ChannelSftp> copy=new HashSet<>(cons);
            for (ChannelSftp con : copy) {
                try {
                    disconnect(con);
                }catch (IOException e){
                    ConnectionInfo info = con2infoMap.get(con);
                    LOG.error("Error encountered while closing connection to "+info.getHost(),e);
                }
            }
        }
        this.idleConnection=null;
        this.con2infoMap=null;
    }

    public synchronized int getMaxConnection(){return maxConnection;}
    public synchronized void setMaxConnection(int maxConn){this.maxConnection=maxConn;}
    public ChannelSftp connect(String host,int port,String user,String password,String keyFile)throws IOException{
        ConnectionInfo info = new ConnectionInfo(host, port, user);
        ChannelSftp channel = getFromPool(info);
        if (channel != null) {
            if (channel.isConnected()) {
                return channel;
            }else {
                channel=null;
                synchronized (this){
                    --liveConnectionCount;
                    con2infoMap.remove(channel);
                }
            }
        }
        JSch jSch = new JSch();
        Session session=null;
        try {
            if (user == null || user.length() == 0) {
                user=System.getProperty("user.name");
            }
            if (password == null) {
                password="";
            }
            if (keyFile != null && keyFile.length() > 0) {
                jSch.addIdentity(keyFile);
            }

            if (port <= 0) {
                session=jSch.getSession(user,host);
            }else {
                session=jSch.getSession(user,host,port);
            }
            session.setPassword(password);
            Properties config = new Properties();
            config.put("StrictHostKeyChecking","no");
            session.setConfig(config);
            session.connect();
            channel=(ChannelSftp) session.openChannel("sftp");
            channel.connect();
            synchronized (this){
                con2infoMap.put(channel,info);
                liveConnectionCount++;
            }
            return channel;
        }catch (JSchException e){
            throw new IOException(StringUtils.stringifyException(e));
        }
    }

    void disconnect(ChannelSftp channel)throws IOException{
        if (channel != null) {
            boolean closeConnection=false;
            synchronized (this){
                if (liveConnectionCount > maxConnection){
                    --liveConnectionCount;
                    con2infoMap.remove(channel);
                    closeConnection=true;
                }
            }
            if (closeConnection) {
                if (channel.isConnected()) {
                    try {
                        Session session = channel.getSession();
                        channel.disconnect();
                        session.disconnect();
                    }catch (JSchException e){
                        throw new IOException(StringUtils.stringifyException(e));
                    }
                }
            }else {
                returnToPool(channel);
            }
        }
    }


    public int getIdleCount() {
        return this.idleConnection.size();
    }

    public int getLiveConnCount() {
        return this.liveConnectionCount;
    }

    public int getConnPoolSize() {
        return this.con2infoMap.size();
    }





    static class ConnectionInfo{
        private String host="";
        private int port;
        private String user="";

        public ConnectionInfo(String host, int port, String user) {
            this.host = host;
            this.port = port;
            this.user = user;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj instanceof ConnectionInfo) {
                ConnectionInfo con = (ConnectionInfo) obj;

                boolean ret = true;
                if (this.host == null || !this.host.equalsIgnoreCase(con.host)) {
                    ret = false;
                }
                if (this.port >= 0 && this.port != con.port) {
                    ret = false;
                }
                if (this.user == null || !this.user.equalsIgnoreCase(con.user)) {
                    ret = false;
                }
                return ret;
            } else {
                return false;
            }

        }

        @Override
        public int hashCode() {
            int hashCode = 0;
            if (host != null) {
                hashCode += host.hashCode();
            }
            hashCode += port;
            if (user != null) {
                hashCode += user.hashCode();
            }
            return hashCode;
        }
    }
}
