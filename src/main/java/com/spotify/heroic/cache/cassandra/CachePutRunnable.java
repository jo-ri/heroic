package com.spotify.heroic.cache.cassandra;

import java.util.List;

import com.codahale.metrics.Timer;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.spotify.heroic.aggregator.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CallbackRunnable;
import com.spotify.heroic.cache.model.CacheBackendKey;
import com.spotify.heroic.cache.model.CacheBackendPutResult;
import com.spotify.heroic.model.CacheKey;
import com.spotify.heroic.model.CacheKeySerializer;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.TimeSerie;

final class CachePutRunnable extends
        CallbackRunnable<CacheBackendPutResult> {
    private static final String CQL_STMT = "INSERT INTO aggregations_1200 (aggregation_key, data_offset, data_value) VALUES(?, ?, ?)";
    private static final CacheKeySerializer cacheKeySerializer = CacheKeySerializer.get();

    private final Keyspace keyspace;
    private final ColumnFamily<Integer, String> columnFamily;
    private final CacheBackendKey key;
    private final List<DataPoint> datapoints;

    CachePutRunnable(String task, Timer timer,
            Callback<CacheBackendPutResult> callback, Keyspace keyspace, ColumnFamily<Integer, String> columnFamily, CacheBackendKey key,
            List<DataPoint> datapoints) {
        super(task, timer, callback);
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.key = key;
        this.datapoints = datapoints;
    }

    @Override
    public CacheBackendPutResult execute() throws Exception {
        final AggregationGroup aggregation = key.getAggregationGroup();
        final TimeSerie timeSerie = key.getTimeSerie();
        final long width = aggregation.getWidth();
        final long columnWidth = width * CassandraCache.WIDTH;

        for (final DataPoint d : datapoints) {
            int index = (int)((d.getTimestamp() % columnWidth) / width);
            long base = d.getTimestamp() - d.getTimestamp() % columnWidth;
            final CacheKey key = new CacheKey(timeSerie, aggregation, base);
            doPut(key, index, d);
        }

        return new CacheBackendPutResult();
    }

    private void doPut(CacheKey key, Integer dataOffset, DataPoint dataPoint)
            throws ConnectionException {
        keyspace.prepareQuery(columnFamily).withCql(CQL_STMT)
                .asPreparedStatement().withByteBufferValue(key, cacheKeySerializer)
                .withIntegerValue(dataOffset)
                .withDoubleValue(dataPoint.getValue()).execute();
    }
}