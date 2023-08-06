package org.apache.hadoop.fs.statistics.impl;

import org.apache.hadoop.fs.statistics.DurationTracker;

import java.time.Duration;

public final class StubDurationTracker implements DurationTracker {
    public static final DurationTracker STUB_DURATION_TRACKER=new StubDurationTracker();

    private StubDurationTracker(){}

    @Override
    public void failed() {

    }

    @Override
    public void close() {

    }

    @Override
    public Duration asDuration() {
        return Duration.ZERO;
    }
}
