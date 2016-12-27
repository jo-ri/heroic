/*
 * Copyright (c) 2017 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.common;

import com.google.common.collect.ImmutableList;

import java.util.List;

import lombok.Data;

@Data
public class TimeRange {

    private final long start;
    private final long end;
    private final boolean openStart;

    public long duration() {
        return end - start;
    }

    public DateRange asClosedStartDateRange() {
        return openStart ? new DateRange(start + 1, end + 1) : new DateRange(start, end);
    }

    public DateRange asOpenStartDateRange() {
        return openStart ? new DateRange(start, end) : new DateRange(start - 1, end - 1);
    }

    public static TimeRange withClosedStart(long start, long end) {
        return new TimeRange(start, end, false);
    }

    public static TimeRange withOpenStart(long start, long end) {
        return new TimeRange(start, end, true);
    }

    public List<TimeRange> splitAtPeriodBoundary(long periodLength) {
        ImmutableList.Builder<TimeRange> result = ImmutableList.builder();
        long startAt = start;
        long firstSplitAt = (start / periodLength + 1) * periodLength;
        for (long splitAt = firstSplitAt; splitAt < end; splitAt += periodLength) {
            result.add(new TimeRange(startAt, splitAt, openStart));
            startAt = splitAt;
        }
        result.add(new TimeRange(startAt, end, openStart));
        return result.build();
    }
}
