package org.apache.hadoop.fs.permission;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.text.MessageFormat;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class FsCreateModes  extends FsPermission{
    private static final long serialVersionUID=0x22986f6d;
    private final FsPermission unmasked;

    public static FsPermission applyUMask(FsPermission mode,
                                          FsPermission umask){
        if (mode.getUnmasked() !=null){
            return mode;
        }
        return create(mode.applyUMask(umask),mode);
    }

    public static FsCreateModes create(FsPermission masked,FsPermission unmasked){
        assert masked.getUnmasked()==null;
        assert unmasked.getUnmasked()==null;
        return new FsCreateModes(masked,unmasked);
    }
    public FsCreateModes(FsPermission masked, FsPermission unmasked){
        super(masked);
        this.unmasked=unmasked;
        assert masked.getUnmasked()==null;
        assert unmasked.getUnmasked()==null;
    }

    @Override
    public FsPermission getMasked() {
        return this;
    }

    @Override
    public FsPermission getUnmasked() {
        return unmasked;
    }
    @Override
    public String toString() {
        return MessageFormat.format("'{' masked: {0}, unmasked: {1} '}'",
                super.toString(), getUnmasked());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        FsCreateModes that = (FsCreateModes) o;
        return getUnmasked().equals(that.getUnmasked());
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + getUnmasked().hashCode();
        return result;
    }
}
