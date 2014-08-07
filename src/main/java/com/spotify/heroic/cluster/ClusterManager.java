package com.spotify.heroic.cluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.CancelledCallback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.async.FailedCallback;
import com.spotify.heroic.async.ResolvedCallback;
import com.spotify.heroic.cluster.async.ClusterNodeLogHandle;
import com.spotify.heroic.cluster.async.NodeRegistryEntryReducer;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.yaml.Utils;
import com.spotify.heroic.yaml.ValidationException;

@Slf4j
@RequiredArgsConstructor
public class ClusterManager {
    public static final class YAML {
        public ClusterDiscovery.YAML discovery;
        public Map<String, String> tags = new HashMap<String, String>();

        public ClusterManager build(String context) throws ValidationException {
            Utils.notNull(context + ".discovery", discovery);
            Utils.notEmpty(context + ".tags", tags);
            final ClusterDiscovery discovery = this.discovery.build(context + ".discovery");
            return new ClusterManager(discovery, UUID.randomUUID(), tags);
        }
    }

    private static final CancelReason REGISTRY_NOT_READY = new CancelReason("registry is not ready yet");

    private final ClusterDiscovery discovery;

    @Getter
    private final UUID localNodeId;
    @Getter
    private final Map<String, String> localNodeTags;

    private final AtomicReference<NodeRegistry> registry = new AtomicReference<>();

    public final Callback<NodeRegistryEntry> findNode(
            final Map<String, String> tags) {
        final NodeRegistry registry = this.registry.get();

        if (registry == null) {
            return new CancelledCallback<>(REGISTRY_NOT_READY);
        }

        final NodeRegistryEntry entry = registry.findEntry(tags);

        if (entry == null) {
            return new FailedCallback<>(new Exception("node not found"));
        }

        return new ResolvedCallback<>(entry);
    }

    public Callback<Void> refresh() throws Exception {
        log.info("Cluster refresh in progress");
        final Callback<Collection<ClusterNode>> callback = discovery.getNodes();
        return callback
                .transform(new Callback.DeferredTransformer<Collection<ClusterNode>, Void>() {
                    @Override
                    public Callback<Void> transform(Collection<ClusterNode> result)
                            throws Exception {
                        final List<Callback<NodeRegistryEntry>> callbacks = new ArrayList<>();

                        for (final ClusterNode clusterNode : result) {
                            final Callback<NodeRegistryEntry> transform = clusterNode.getMetadata().transform(new Callback.Transformer<NodeMetadata, NodeRegistryEntry>() {
                                @Override
                                public NodeRegistryEntry transform(NodeMetadata result)
                                        throws Exception {
                                    return new NodeRegistryEntry(clusterNode, result);
                                }
                            });
                            transform.register(new ClusterNodeLogHandle(
                                    clusterNode));
                            callbacks.add(transform);
                        }
                        return ConcurrentCallback.newReduce(callbacks,
                                new NodeRegistryEntryReducer(registry));
                    }
                });
    }
}