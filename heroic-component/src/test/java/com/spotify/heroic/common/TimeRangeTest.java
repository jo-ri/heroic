package com.spotify.heroic.common;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

public class TimeRangeTest {

    @Test
    public void testSplitAtPeriodBoundary() {
        for (int period = 1; period < 20; period++) {
            for (int start = 0; start < 100; start++) {
                for (int end = start; end < 100; end++) {
                    assertTimeRangeSplitCorrectly(TimeRange.withClosedStart(start, end), period);
                    assertTimeRangeSplitCorrectly(TimeRange.withOpenStart(start, end), period);
                }
            }
        }
    }

    private void assertTimeRangeSplitCorrectly(TimeRange range, int period) {
        List<TimeRange> periods = range.splitAtPeriodBoundary(period);
        assertFalse("At least one range must be returned", periods.isEmpty());
        assertEquals(range.getStart(), periods.get(0).getStart());
        assertEquals(range.isOpenStart(), periods.get(0).isOpenStart());
        for (int i = 1; i < periods.size(); i++) {
            assertEquals(range.isOpenStart(), periods.get(i).isOpenStart());
            long splitBoundary = periods.get(i).getStart();
            assertEquals(periods.get(i - 1).getEnd(), splitBoundary);
            assertEquals(0, splitBoundary % period);
        }
        assertEquals(range.getEnd(), periods.get(periods.size() - 1).getEnd());
    }
}
