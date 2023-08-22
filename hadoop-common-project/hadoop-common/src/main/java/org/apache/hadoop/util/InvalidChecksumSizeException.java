package org.apache.hadoop.util;

import java.io.IOException;

public class InvalidChecksumSizeException extends IOException {

    private static final long serialVersionUID = 1L;

    public InvalidChecksumSizeException(String s) {
        super(s);
    }
}
