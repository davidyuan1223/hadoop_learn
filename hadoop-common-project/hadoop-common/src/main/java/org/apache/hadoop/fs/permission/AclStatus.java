package org.apache.hadoop.fs.permission;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.thirdparty.com.google.common.base.Objects;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;


@InterfaceAudience.Public
@InterfaceStability.Stable
/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/30
 **/
public class AclStatus {
    private final String owner;
    private final String group;
    private final boolean stickyBit;
    private final List<AclEntry> entries;
    private final FsPermission permission;

    public String getOwner() {
        return owner;
    }

    public String getGroup() {
        return group;
    }
    public boolean isStickyBit(){
        return stickyBit;
    }

    public List<AclEntry> getEntries() {
        return entries;
    }

    public FsPermission getPermission() {
        return permission;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        AclStatus other=(AclStatus) o;
        return Objects
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(owner, group, stickyBit, entries, permission);
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("owner: ")
                .append(owner)
                .append(", group: ")
                .append(group)
                .append(", acl: {")
                .append("entries: ")
                .append(entries)
                .append(", stickyBit: ")
                .append(stickyBit)
                .append('}')
                .toString();
    }

    public static class Builder{
        private String owner;
        private String group;
        private boolean stickyBit;
        private List<AclEntry> entries=new ArrayList<>();
        private FsPermission permission=null;
        public Builder owner(String owner) {
            this.owner = owner;
            return this;
        }

        /**
         * Sets the file group.
         *
         * @param group String file group
         * @return Builder this builder, for call chaining
         */
        public Builder group(String group) {
            this.group = group;
            return this;
        }

        /**
         * Adds an ACL entry.
         *
         * @param e AclEntry entry to add
         * @return Builder this builder, for call chaining
         */
        public Builder addEntry(AclEntry e) {
            this.entries.add(e);
            return this;
        }

        /**
         * Adds a list of ACL entries.
         *
         * @param entries AclEntry entries to add
         * @return Builder this builder, for call chaining
         */
        public Builder addEntries(Iterable<AclEntry> entries) {
            for (AclEntry e : entries)
                this.entries.add(e);
            return this;
        }

        /**
         * Sets sticky bit. If this method is not called, then the builder assumes
         * false.
         *
         * @param stickyBit
         *          boolean sticky bit
         * @return Builder this builder, for call chaining
         */
        public Builder stickyBit(boolean stickyBit) {
            this.stickyBit = stickyBit;
            return this;
        }

        /**
         * Sets the permission for the file.
         * @param permission permission.
         * @return Builder.
         */
        public Builder setPermission(FsPermission permission) {
            this.permission = permission;
            return this;
        }

        /**
         * Builds a new AclStatus populated with the set properties.
         *
         * @return AclStatus new AclStatus
         */
        public AclStatus build() {
            return new AclStatus(owner, group, stickyBit, entries, permission);
        }
    }
    private AclStatus(String owner,String group,boolean stickyBit,Iterable<AclEntry> entries,FsPermission permission){
        this.owner=owner;
        this.group=group;
        this.stickyBit=stickyBit;
        this.entries= Lists.newArrayList(entries);
        this.permission=permission;
    }
    public FsAction getEffectivePermission(AclEntry entry,FsPermission permArg)throws IllegalArgumentException{
        Preconditions.checkArgument(this.permission!=null || permArg!=null,
                "Permission bits are not available to calculate effective permission");
        if (this.permission != null) {
            permArg=this.permission;
        }
        if ((entry.getName() != null || entry.getType() == AclEntryType.GROUP)) {
            if (entry.getScope() == AclEntryScope.ACCESS) {
                FsAction entryPerm = entry.getPermission();
                return entryPerm.and(permArg.getGroupAction());
            }else {
                Preconditions.checkArgument(this.entries.contains(entry)&&this.entries.size()>=3,
                        "Passed default ACL entry not found in the list of ACLS");
                FsAction defaultMask = this.entries.get(this.entries.size() - 2).getPermission();
                FsAction entryPerm = entry.getPermission();
                return entryPerm.and(defaultMask);
            }
        }else {
            return entry.getPermission();
        }
    }
}
