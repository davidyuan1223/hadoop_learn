package org.apache.hadoop.fs.ptotocolPB;

import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;

public final class PBHelper {
    private PBHelper(){}
    public static FsPermission convert(FsPermissionProto proto)throws IOException{
        return new FsPermission((short)proto.getPerm());
    }
}
