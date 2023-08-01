package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;
import java.util.List;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface BatchListingOperations {
    /**
     *
     * @param paths
     * @return
     * @throws IOException
     */
    RemoteIterator<PartialListing<FileStatus>> batchedListStatusIterator(List<Path> paths)throws IOException;
    RemoteIterator<PartialListing<LocatedFileStatus>> batchedLocatedListStatusIterator(List<Path> paths)throws IOException;

}
