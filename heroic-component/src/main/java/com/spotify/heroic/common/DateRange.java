

/*
 * Copyright (c) 2015 Spotify AB.
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import eu.toolchain.serializer.AutoSerialize;

import org.apache.commons.lang3.time.FastDateFormat;

import java.sql.Date;

import lombok.Data;
import lombok.EqualsAndHashCode;

import static com.google.common.base.Preconditions.checkArgument;

@AutoSerialize
@Data
@EqualsAndHashCode(of = {"start", "end"})
public class DateRange {
    private static final FastDateFormat FORMAT =
        FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS");

    private final long start;
    private final long end;

    public DateRange(long start, long end) {
        checkArgument(start >= 0, "start must be a positive number");

        if (end < start) {
            throw new IllegalArgumentException(
                String.format("start (%d) must come before end (%d)", start, end));
        }

        this.start = start;
        this.end = end;
    }

    public long start() {
        return start;
    }

    public long end() {
        return end;
    }

    /**
     * Creates a range that is rounded to the specified interval.
     *
     * @param interval Interval to round to. Return same range if 0.
     * @return Rounded date range.
     */
    public DateRange rounded(long interval) {
        if (interval <= 0) {
            return this;
        }

        return new DateRange(start - start % interval, end - (end  % interval));
    }

    public DateRange shift(long extent) {
        return new DateRange(Math.max(start + extent, 0), Math.max(end + extent, 0));
    }

    @Override
    public String toString() {
        final Date start = new Date(this.start);
        final Date end = new Date(this.end);
        return "{" + FORMAT.format(start) + "}-{" + FORMAT.format(end) + "}";
    }

    @JsonCreator
    public static DateRange create(
        @JsonProperty(value = "start", required = true) Long start,
        @JsonProperty(value = "end", required = true) Long end
    ) {
        return new DateRange(start, end);
    }

    public static DateRange now(long now) {
        return new DateRange(now, now);
    }

    public static DateRange now() {
        return now(System.currentTimeMillis());
    }
}
