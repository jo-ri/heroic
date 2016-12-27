package com.spotify.heroic.aggregation;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.common.TimeRange;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class BucketAggregationTest {
    public final class IterableBuilder {
        final ArrayList<Point> datapoints = new ArrayList<Point>();

        public IterableBuilder add(long timestamp, double value) {
            datapoints.add(new Point(timestamp, value));
            return this;
        }

        public List<Point> result() {
            return datapoints;
        }
    }

    public IterableBuilder build() {
        return new IterableBuilder();
    }

    @Data
    @EqualsAndHashCode(callSuper = true)
    public static class TestBucket extends AbstractBucket {
        private final long timestamp;
        private double sum;

        public void updatePoint(Map<String, String> key, Point d) {
            sum += d.getValue();
        }

        @Override
        public long timestamp() {
            return timestamp;
        }
    }

    public BucketAggregationInstance<TestBucket> setup(long size, long extent) {
        return new BucketAggregationInstance<TestBucket>(size, extent,
            ImmutableSet.of(MetricType.POINT), MetricType.POINT) {
            @Override
            protected TestBucket buildBucket(long timestamp) {
                return new TestBucket(timestamp);
            }

            @Override
            protected Point build(TestBucket bucket) {
                return new Point(bucket.timestamp, bucket.sum);
            }

            @Override
            public AggregationInstance distributed() {
                return this;
            }

            @Override
            public AggregationInstance reducer() {
                return Mockito.mock(AggregationInstance.class);
            }
        };
    }

    final Map<String, String> group = ImmutableMap.of();
    final Set<Series> series = ImmutableSet.of();

    @Test
    public void testSameSampling() {
        List<Point> input = build()
            .add(1000, 1.1)
            .add(1999, 1.2)
            .add(2000, 1.2)
            .add(2000, 1.3)
            .add(2001, 1.8)
            .result();
        List<Point> expected = build().add(2000, 3.7).add(3000, 1.8).result();
        checkBucketAggregation(input, expected, 1000);
    }

    @Test
    public void testLongerExtent() {
        List<Point> input =
            build().add(0, 1.0).add(999, 1.0).add(1000, 1.0).add(2000, 1.0).add(2001, 1.1).result();
        List<Point> expected = build().add(2000, 3.0).add(3000, 2.1).result();
        checkBucketAggregation(input, expected, 2000);
    }

    @Test
    public void testShorterExtent() {
        final List<Point> input = build()
            .add(1499, 1.0)
            .add(1500, 1.1)
            .add(1501, 1.1)
            .add(2000, 1.0).add(2001, 1.0)
            .add(2500, 1.2).add(2501, 1.5)
            .result();
        final List<Point> expected = build().add(2000, 2.1).add(3000, 1.5).result();
        checkBucketAggregation(input, expected, 500);
    }

    private void checkBucketAggregation(
        List<Point> input, List<Point> expected, final long extent
    ) {
        final BucketAggregationInstance<TestBucket> a = setup(1000, extent);
        final AggregationSession session = a.session(TimeRange.withOpenStart(1000, 3000));
        session.updatePoints(group, series, input);

        final AggregationResult result = session.result();

        Assert.assertEquals(expected, result.getResult().get(0).getMetrics().getData());
    }

    @Test
    public void testUnevenSampling() {
        final BucketAggregationInstance<TestBucket> a = setup(999, 499);
        final AggregationSession session = a.session(TimeRange.withOpenStart(1000, 2998));
        session.updatePoints(group, series, build()
            .add(1500, 1.0)
            .add(1501, 1.1)
            .add(1999, 1.2)
            .add(2998, 1.4)
            .add(2999, 1.7)
            .result());

        final AggregationResult result = session.result();

        Assert.assertEquals(build().add(1999, 2.3).add(2998, 1.4).result(),
            result.getResult().get(0).getMetrics().getData());
    }

    @Test
    public void testShorterClosedStart() {
        final BucketAggregationInstance<TestBucket> a = setup(999, 499);
        final AggregationSession session = a.session(TimeRange.withClosedStart(1501, 3499));
        session.updatePoints(group, series, build()
            .add(1500, 1.0)
            .add(1501, 1.1)
            .add(1999, 1.2)
            .add(2998, 1.4)
            .add(2999, 1.7)
            .result());

        final AggregationResult result = session.result();

        Assert.assertEquals(build().add(1501, 2.3).add(2500, 1.4).result(),
            result.getResult().get(0).getMetrics().getData());
    }

    @Test
    public void testLongerClosedStart() {
        final BucketAggregationInstance<TestBucket> a = setup(1000, 2000);
        final AggregationSession session = a.session(TimeRange.withClosedStart(1000, 3000));

        session.updatePoints(group, series, build()
            .add(0, 1.0)
            .add(999, 1.0)
            .add(1000, 1.0)
            .add(2000, 1.0)
            .add(2001, 1.2)
            .add(3000, 1.3)
            .add(4000, 1.1)
            .result());

        final AggregationResult result = session.result();

        Assert.assertEquals(build().add(1000, 3.2).add(2000, 3.5).result(),
            result.getResult().get(0).getMetrics().getData());
    }
}
