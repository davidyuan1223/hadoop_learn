package org.apache.hadoop.fs.permission;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
@InterfaceStability.Unstable
public class PermissionStatus implements Writable {
    static final WritableFactory FACTORY=new WritableFactory() {
        @Override
        public Writable newInstance() {
            return new PermissionStatus();
        }
    };
    static {
        WritableFactories.setFactory(PermissionStatus.class,FACTORY);
    }
    private PermissionStatus(){}
    private String username;
    private String groupname;
    private FsPermission permission;
    public static PermissionStatus createImmutable(String user,String group,FsPermission permission){
        return new PermissionStatus(user,group,permission){
            @Override
            public void readFields(DataInput in) throws IOException {
                throw new UnsupportedOperationException();
            }
        };
    }
    public PermissionStatus(String user,String group, FsPermission permission){
        username=user;
        groupname=group;
        this.permission=permission;
    }

    public String getUsername() {
        return username;
    }

    public String getGroupname() {
        return groupname;
    }

    public FsPermission getPermission() {
        return permission;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        username= Text.readString(in,Text.DEFAULT_MAX_LEN);
        groupname=Text.readString(in,Text.DEFAULT_MAX_LEN);
        permission=FsPermission.read(in);
    }

    @Override
    public void writer(DataOutput out) throws IOException {
        write(out,username,groupname,permission);
    }
    public static PermissionStatus read(DataInput in)throws IOException{
        PermissionStatus p = new PermissionStatus();
        p.readFields(in);
        return p;
    }
    public static void write(DataOutput out,String username,String groupname,FsPermission permission)throws IOException{
        Text.writeString(out,username,Text.DEFAULT_MAX_LEN);
        Text.writeString(out,groupname,Text.DEFAULT_MAX_LEN);
        permission.writer(out);
    }
    @Override
    public String toString() {
        return username + ":" + groupname + ":" + permission;
    }
}
