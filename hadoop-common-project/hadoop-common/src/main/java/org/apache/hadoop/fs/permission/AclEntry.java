package org.apache.hadoop.fs.permission;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.util.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;


@InterfaceAudience.Public
@InterfaceStability.Stable
/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/30
 **/
public class AclEntry {
    private final AclEntryType type;
    private final String name;
    private final FsAction permission;
    private final AclEntryScope scope;

    public AclEntryType getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public FsAction getPermission() {
        return permission;
    }

    public AclEntryScope getScope() {
        return scope;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        AclEntry other = (AclEntry)o;
        return org.apache.hadoop.thirdparty.com.google.common.base.Objects.equal(type, other.type) &&
                org.apache.hadoop.thirdparty.com.google.common.base.Objects.equal(name, other.name) &&
                org.apache.hadoop.thirdparty.com.google.common.base.Objects.equal(permission, other.permission) &&
                org.apache.hadoop.thirdparty.com.google.common.base.Objects.equal(scope, other.scope);    }

    @Override
    public int hashCode() {
        return Objects.hash(type, name, permission, scope);
    }
    @Override
    @InterfaceStability.Unstable
    public String toString() {
        // This currently just delegates to the stable string representation, but it
        // is permissible for the output of this method to change across versions.
        return toStringStable();
    }

    /**
     * Returns a string representation guaranteed to be stable across versions to
     * satisfy backward compatibility requirements, such as for shell command
     * output or serialization.  The format of this string representation matches
     * what is expected by the {@link #parseAclSpec(String, boolean)} and
     * {@link #parseAclEntry(String, boolean)} methods.
     *
     * @return stable, backward compatible string representation
     */
    public String toStringStable() {
        StringBuilder sb = new StringBuilder();
        if (scope == AclEntryScope.DEFAULT) {
            sb.append("default:");
        }
        if (type != null) {
            sb.append(StringUtils.toLowerCase(type.toStringStable()));
        }
        sb.append(':');
        if (name != null) {
            sb.append(name);
        }
        sb.append(':');
        if (permission != null) {
            sb.append(permission.SYMBOL);
        }
        return sb.toString();
    }

    public static class Builder{
        private AclEntryType type;
        private String name;
        private FsAction permission;
        private AclEntryScope scope=AclEntryScope.ACCESS;

        public Builder setType(AclEntryType type) {
            this.type = type;
            return this;
        }
        public Builder setName(String name) {
            if (name != null && !name.isEmpty()) {
                this.name = name;
            }
            return this;
        }

        /**
         * Sets the set of permissions in the ACL entry.
         *
         * @param permission FsAction set of permissions in the ACL entry
         * @return Builder this builder, for call chaining
         */
        public Builder setPermission(FsAction permission) {
            this.permission = permission;
            return this;
        }

        /**
         * Sets the scope of the ACL entry.  If this method is not called, then the
         * builder assumes {@link AclEntryScope#ACCESS}.
         *
         * @param scope AclEntryScope scope of the ACL entry
         * @return Builder this builder, for call chaining
         */
        public Builder setScope(AclEntryScope scope) {
            this.scope = scope;
            return this;
        }

        /**
         * Builds a new AclEntry populated with the set properties.
         *
         * @return AclEntry new AclEntry
         */
        public AclEntry build() {
            return new AclEntry(type, name, permission, scope);
        }
    }
    private AclEntry(AclEntryType type, String name, FsAction permission, AclEntryScope scope) {
        this.type = type;
        this.name = name;
        this.permission = permission;
        this.scope = scope;
    }
    public static List<AclEntry> parseAclSpec(String aclSpec,boolean includePermission){
        List<AclEntry> aclEntries=new ArrayList<>();
        Collection<String> aclStrings = StringUtils.getStringCollection(aclSpec, ",");
        for (String aclStr : aclStrings) {
            AclEntry aclEntry=parseAclEntry(aclStr,includePermission);
            aclEntries.add(aclEntry);
        }
        return aclEntries;
    }
    public static AclEntry parseAclEntry(String aclStr,boolean includePermission){
        AclEntry.Builder builder = new Builder();
        String[] split = aclStr.split(":");
        if (split.length==0) {
            throw new HadoopIllegalArgumentException("Invalid <aclSpec> : "+aclStr);
        }
        int index=0;
        if ("default".equals(split[0])) {
            index++;
            builder.setScope(AclEntryScope.DEFAULT);
        }
        if (split.length<=index) {
            throw new HadoopIllegalArgumentException("Invalid <aclSpec> : "+aclStr);
        }
        AclEntryType aclEntryType=null;
        try {
            aclEntryType=Enum.valueOf(AclEntryType.class,StringUtils.toUpperCase(split[index]));
            builder.setType(aclEntryType);
            index++;
        }catch (IllegalArgumentException e){
            throw new HadoopIllegalArgumentException("Invalid type of acl in <aclSpec> : "+aclStr);
        }
        if (split.length > index) {
            String name = split[index];
            if (!name.isEmpty()) {
                builder.setName(name);
            }
            index++;
        }
        if (includePermission) {
            if (split.length <= index) {
                throw new HadoopIllegalArgumentException("Invalid <aclSpec> : "+aclStr);
            }
            String permission = split[index];
            FsAction fsAction=FsAction.getFsAction(permission);
            if (fsAction == null) {
                throw new HadoopIllegalArgumentException("Invalid permission in <aclSpec> : "+aclStr);
            }
            builder.setPermission(permission);
            index++;
        }
        if (split.length > index) {
            throw new HadoopIllegalArgumentException("Invalid <aclSpec> : "+aclStr);
        }
        AclEntry aclEntry = builder.build();
        return aclEntry;
    }
    public static String aclSpecToString(List<AclEntry> aclSpec){
        StringBuilder buf = new StringBuilder();
        for (AclEntry aclEntry : aclSpec) {
            buf.append(aclSpec.toString())
                    .append(",");
        }
        return buf.substring(0,buf.length()-1);
    }
}
