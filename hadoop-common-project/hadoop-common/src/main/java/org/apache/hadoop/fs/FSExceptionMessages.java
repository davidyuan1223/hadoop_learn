package org.apache.hadoop.fs;

public class FSExceptionMessages {
    public static final String STREAM_IS_CLOSED="Stream is closed";
    public static final String NEGATIVE_SEEK="Cannot seek to a negative offset";
    public static final String CANNOT_SEEK_PAST_EOF="Attempted to seek or read past the end of the file";
    public static final String EOF_IN_READ_FULLY="End of file reached before reading fully.";
    public static final String TOO_MANY_BYTES_FOR_DEST_BUFFER="Requested more bytes than destination buffer size";
    public static final String PERMISSION_DENIED="Permission denied";
    public static final String PERMISSION_DENIED_BY_STICKY_BIT="Permission denied by sticky bit";
    public static final String ABORTABLE_UNSUPPORTED="Abortable.abort() is not supported";
}
