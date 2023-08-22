package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.net.URI;

@InterfaceAudience.Public
@InterfaceStability.Stable
public interface FsConstants {
    URI LOCAL_FS_URI=URI.create("file://");
    String FTP_SCHEME="ftp";
    int MAX_PATH_LINKS=32;
    URI VIEWFS_URI=URI.create("viewfs://");
    String VIEWFS_SCHEME="viewfs";
    String FS_VIEWFS_OVERLOAD_SCHEME_TARGET_FS_IMPL_PATTERN="fs.viewfs.overload.scheme.target.%s.impl";
    String VIEWFS_TYPE="viewfs";
}
