package org.apache.hadoop.fs.http;

public class HttpFileSystem  extends AbstractHttpFileSystem{
    @Override
    public String getScheme() {
        return "http";
    }
}
