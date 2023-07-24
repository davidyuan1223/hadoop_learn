package org.apache.hadoop.conf;
import java.util.Locale;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
public class StorageSize {
    private final StorageUnit unit;
    private final double value;
    public StorageSize(StorageUnit unit,double value){
        this.unit=unit;
        this.value=value;
    }
    protected static void checkState(boolean state,String errorString){
        if (!state){
            throw new IllegalArgumentException(errorString);
        }
    }
    public static StorageSize parse(String value){
        checkState(isNotBlank(value),"value cannot be blank");
        String sanitizedValue=value.trim().toLowerCase(Locale.ENGLISH);
        StorageUnit parsedUnit=null;
        for (StorageUnit unit : StorageUnit.values()) {
            if (sanitizedValue.endsWith(unit.getShortName())
                    || sanitizedValue.endsWith(unit.getLongName())
                    || sanitizedValue.endsWith(unit.getSuffixChar())) {
                parsedUnit=unit;
                break;
            }
        }
        if (parsedUnit == null) {
            throw new IllegalArgumentException(value+" is not in expected format." +
                    "Expected format is <number><unit>. e.g. 1000MB");
        }
        String suffix="";
        boolean found=false;
        if (!found && sanitizedValue.endsWith(parsedUnit.getLongName())) {
            found=true;
            suffix=parsedUnit.getLongName();
        }
        if (!found && sanitizedValue.endsWith(parsedUnit.getShortName())) {
            found=true;
            suffix=parsedUnit.getShortName();
        }
        if (!found && sanitizedValue.endsWith(parsedUnit.getSuffixChar())) {
            found=true;
            suffix=parsedUnit.getSuffixChar();
        }
        checkState(found,"Something is wrong,we have to find a match.Internal error.");
        String valString = sanitizedValue.substring(0, value.length() - suffix.length());
        return new StorageSize(parsedUnit,Double.parseDouble(valString));
    }

    public StorageUnit getUnit() {
        return unit;
    }

    public double getValue() {
        return value;
    }
}
