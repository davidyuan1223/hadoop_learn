package org.apache.hadoop.conf;

import java.math.BigDecimal;
import java.math.RoundingMode;

public enum StorageUnit {
    EB {
        @Override
        public double toBytes(double value) {
            return multiply(value, EXABYTES);
        }

        @Override
        public double toKBs(double value) {
            return multiply(value, EXABYTES / KILOBYTES);
        }

        @Override
        public double toMBs(double value) {
            return multiply(value, EXABYTES / MEGABYTES);
        }

        @Override
        public double toGBs(double value) {
            return multiply(value, EXABYTES / GIGABYTES);
        }

        @Override
        public double toTBs(double value) {
            return multiply(value, EXABYTES / TERABYTES);
        }

        @Override
        public double toPBs(double value) {
            return multiply(value, EXABYTES / PETABYTES);
        }

        @Override
        public double toEBs(double value) {
            return value;
        }

        @Override
        public String getLongName() {
            return "exabytes";
        }

        @Override
        public String getShortName() {
            return "eb";
        }

        @Override
        public String getSuffixChar() {
            return "e";
        }

        @Override
        public double getDefault(double value) {
            return toEBs(value);
        }

        @Override
        public double fromBytes(double value) {
            return divide(value, EXABYTES);
        }
    },
    PB {
        @Override
        public double toBytes(double value) {
            return multiply(value, PETABYTES);
        }

        @Override
        public double toKBs(double value) {
            return multiply(value, PETABYTES / KILOBYTES);
        }

        @Override
        public double toMBs(double value) {
            return multiply(value, PETABYTES / MEGABYTES);
        }

        @Override
        public double toGBs(double value) {
            return multiply(value, PETABYTES / GIGABYTES);
        }

        @Override
        public double toTBs(double value) {
            return multiply(value, PETABYTES / TERABYTES);
        }

        @Override
        public double toPBs(double value) {
            return value;
        }

        @Override
        public double toEBs(double value) {
            return divide(value, EXABYTES / PETABYTES);
        }

        @Override
        public String getLongName() {
            return "petabytes";
        }

        @Override
        public String getShortName() {
            return "pb";
        }

        @Override
        public String getSuffixChar() {
            return "p";
        }

        @Override
        public double getDefault(double value) {
            return toPBs(value);
        }

        @Override
        public double fromBytes(double value) {
            return divide(value, PETABYTES);
        }
    },
    TB {
        @Override
        public double toBytes(double value) {
            return multiply(value, TERABYTES);
        }

        @Override
        public double toKBs(double value) {
            return multiply(value, TERABYTES / KILOBYTES);
        }

        @Override
        public double toMBs(double value) {
            return multiply(value, TERABYTES / MEGABYTES);
        }

        @Override
        public double toGBs(double value) {
            return multiply(value, TERABYTES / GIGABYTES);
        }

        @Override
        public double toTBs(double value) {
            return value;
        }

        @Override
        public double toPBs(double value) {
            return divide(value, PETABYTES / TERABYTES);
        }

        @Override
        public double toEBs(double value) {
            return divide(value, EXABYTES / TERABYTES);
        }

        @Override
        public String getLongName() {
            return "terabytes";
        }

        @Override
        public String getShortName() {
            return "tb";
        }

        @Override
        public String getSuffixChar() {
            return "t";
        }

        @Override
        public double getDefault(double value) {
            return toTBs(value);
        }

        @Override
        public double fromBytes(double value) {
            return divide(value, TERABYTES);
        }
    },
    GB {
        @Override
        public double toBytes(double value) {
            return multiply(value, GIGABYTES);
        }

        @Override
        public double toKBs(double value) {
            return multiply(value, GIGABYTES / KILOBYTES);
        }

        @Override
        public double toMBs(double value) {
            return multiply(value, GIGABYTES / MEGABYTES);
        }

        @Override
        public double toGBs(double value) {
            return value;
        }

        @Override
        public double toTBs(double value) {
            return divide(value, TERABYTES / GIGABYTES);
        }

        @Override
        public double toPBs(double value) {
            return divide(value, PETABYTES / GIGABYTES);
        }

        @Override
        public double toEBs(double value) {
            return divide(value, EXABYTES / GIGABYTES);
        }

        @Override
        public String getLongName() {
            return "gigabytes";
        }

        @Override
        public String getShortName() {
            return "gb";
        }

        @Override
        public String getSuffixChar() {
            return "g";
        }

        @Override
        public double getDefault(double value) {
            return toGBs(value);
        }

        @Override
        public double fromBytes(double value) {
            return divide(value, GIGABYTES);
        }
    },
    MB {
        @Override
        public double toBytes(double value) {
            return multiply(value, MEGABYTES);
        }

        @Override
        public double toKBs(double value) {
            return multiply(value, MEGABYTES / KILOBYTES);
        }

        @Override
        public double toMBs(double value) {
            return value;
        }

        @Override
        public double toGBs(double value) {
            return divide(value, GIGABYTES / MEGABYTES);
        }

        @Override
        public double toTBs(double value) {
            return divide(value, TERABYTES / MEGABYTES);
        }

        @Override
        public double toPBs(double value) {
            return divide(value, PETABYTES / MEGABYTES);
        }

        @Override
        public double toEBs(double value) {
            return divide(value, EXABYTES / MEGABYTES);
        }

        @Override
        public String getLongName() {
            return "megabytes";
        }

        @Override
        public String getShortName() {
            return "mb";
        }

        @Override
        public String getSuffixChar() {
            return "m";
        }

        @Override
        public double fromBytes(double value) {
            return divide(value, MEGABYTES);
        }

        @Override
        public double getDefault(double value) {
            return toMBs(value);
        }
    },
    KB {
        @Override
        public double toBytes(double value) {
            return multiply(value, KILOBYTES);
        }

        @Override
        public double toKBs(double value) {
            return value;
        }

        @Override
        public double toMBs(double value) {
            return divide(value, MEGABYTES / KILOBYTES);
        }

        @Override
        public double toGBs(double value) {
            return divide(value, GIGABYTES / KILOBYTES);
        }

        @Override
        public double toTBs(double value) {
            return divide(value, TERABYTES / KILOBYTES);
        }

        @Override
        public double toPBs(double value) {
            return divide(value, PETABYTES / KILOBYTES);
        }

        @Override
        public double toEBs(double value) {
            return divide(value, EXABYTES / KILOBYTES);
        }

        @Override
        public String getLongName() {
            return "kilobytes";
        }

        @Override
        public String getShortName() {
            return "kb";
        }

        @Override
        public String getSuffixChar() {
            return "k";
        }

        @Override
        public double getDefault(double value) {
            return toKBs(value);
        }

        @Override
        public double fromBytes(double value) {
            return divide(value, KILOBYTES);
        }
    },
    BYTES {
        @Override
        public double toBytes(double value) {
            return value;
        }

        @Override
        public double toKBs(double value) {
            return divide(value, KILOBYTES);
        }

        @Override
        public double toMBs(double value) {
            return divide(value, MEGABYTES);
        }

        @Override
        public double toGBs(double value) {
            return divide(value, GIGABYTES);
        }

        @Override
        public double toTBs(double value) {
            return divide(value, TERABYTES);
        }

        @Override
        public double toPBs(double value) {
            return divide(value, PETABYTES);
        }

        @Override
        public double toEBs(double value) {
            return divide(value, EXABYTES);
        }

        @Override
        public String getLongName() {
            return "bytes";
        }

        @Override
        public String getShortName() {
            return "b";
        }

        @Override
        public String getSuffixChar() {
            return "b";
        }

        @Override
        public double getDefault(double value) {
            return toBytes(value);
        }

        @Override
        public double fromBytes(double value) {
            return value;
        }
    };

    private static final double BYTE=1L;
    private static final double KILOBYTES=BYTE*1024L;
    private static final double MEGABYTES=KILOBYTES*1024L;
    private static final double GIGABYTES=MEGABYTES*1024L;
    private static final double TERABYTES=GIGABYTES*1024L;
    private static final double PETABYTES=TERABYTES*1024L;
    private static final double EXABYTES=PETABYTES*1024L;
    private static final int PRECISION=4;

    private static double divide(double value,double divisor){
        BigDecimal val = new BigDecimal(value);
        BigDecimal div = new BigDecimal(divisor);
        return val.divide(div)
                .setScale(PRECISION, RoundingMode.HALF_UP)
                .doubleValue();
    }
    private static double multiply(double first,double second){
        BigDecimal fir = new BigDecimal(first);
        BigDecimal sec = new BigDecimal(second);
        return fir.multiply(sec)
                .setScale(PRECISION,RoundingMode.HALF_UP)
                .doubleValue();
    }
    public abstract double toBytes(double value);
    public abstract double toKBs(double value);
    public abstract double toMBs(double value);
    public abstract double toGBs(double value);
    public abstract double toTBs(double value);
    public abstract double toPBs(double value);
    public abstract double toEBs(double value);
    public abstract String getLongName();
    public abstract String getShortName();
    public abstract String getSuffixChar();
    public abstract double getDefault(double value);
    public abstract double fromBytes(double value);
    public String toString(){
        return getLongName();
    }
}
