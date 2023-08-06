package org.apache.hadoop.fs.statistics.impl;

import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;

public class StubDurationTrackerFactory  implements DurationTrackerFactory {

    public static final StubDurationTrackerFactory STUB_DURATION_TRACKER_FACTORY=new StubDurationTrackerFactory();
    private StubDurationTrackerFactory(){}

    @Override
    public DurationTracker trackDuration(String key, long count) {
        return StubDurationTracker.STUB_DURATION_TRACKER;
    }
}
