package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class MultipleIOException extends IOException {
    private static final long serialVersionUID=1L;
    private final List<IOException> exceptions;
    private MultipleIOException(List<IOException> exceptions){
        super(exceptions.size()+" exceptions "+exceptions);
        this.exceptions=exceptions;
    }

    public List<IOException> getExceptions() {
        return exceptions;
    }
    public static IOException createIOException(List<IOException> exceptions){
        if (exceptions == null || exceptions.isEmpty()) {
            return null;
        }
        if (exceptions.size() == 1) {
            return exceptions.get(0);
        }
        return new MultipleIOException(exceptions);
    }
    public static class Builder{
        private List<IOException> exceptions;
        public void add(Throwable t){
            if (exceptions == null) {
                exceptions=new ArrayList<>();
            }
            exceptions.add(t instanceof IOException?(IOException) t:new IOException(t));
        }
        public IOException build(){
            return createIOException(exceptions);
        }
        public boolean isEmpty(){
            if (exceptions == null) {
                return true;
            }
            return exceptions.isEmpty();
        }
    }

}
