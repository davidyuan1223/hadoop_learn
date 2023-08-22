package org.apache.hadoop.fs.http;

public class HttpsFileSystem extends AbstractHttpFileSystem{
    @Override
    public String getScheme() {
        return "https";
    }
}
