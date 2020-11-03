/**
 * Copyright 2019 VMware, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micrometer.core.instrument.binder.mongodb;

import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import io.micrometer.core.annotation.Incubating;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.lang.NonNullApi;
import io.micrometer.core.lang.NonNullFields;
import io.micrometer.core.lang.Nullable;
import io.micrometer.core.util.internal.logging.InternalLogger;
import io.micrometer.core.util.internal.logging.InternalLoggerFactory;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * {@link CommandListener} for collecting command metrics from {@code MongoClient}.
 *
 * @author Christophe Bornet
 * @author Chris Bono
 * @since 1.2.0
 */
@NonNullApi
@NonNullFields
@Incubating(since = "1.2.0")
public class MongoMetricsCommandListener implements CommandListener {

    private static final InternalLogger LOG = InternalLoggerFactory.getInstance(MongoMetricsCommandListener.class);

    private final MeterRegistry registry;
    private final String timerMetricName;
    private final String timerMetricDescription;
    // The following is all related to extracting collection name
    private final MongoCommandUtil mongoCommandUtil = new MongoCommandUtil();
    private final ConcurrentHashMap<Integer, String> collectionNamesCache = new ConcurrentHashMap<>();
    private final int collectionNamesCacheMaxSize;
    @Nullable // VisibleForTesting
    final EveryNthOperation collectionNamesCacheOverflowErrorTracker;

    /**
     * Constructs a command listener with default values.
     *
     * @param registry meter registry
     * @deprecated use {@link MongoMetricsCommandListener.Builder} instead
     */
    @Deprecated
    public MongoMetricsCommandListener(MeterRegistry registry) {
        this(MongoMetricsCommandListener.builder(registry));
    }

    private MongoMetricsCommandListener(Builder builder) {
        this.registry = builder.registry;
        this.timerMetricName = builder.timerMetricName;
        this.timerMetricDescription = builder.timerMetricDescription;
        if (builder.collectionNamesCacheMaxSize < 1) {
            throw new IllegalArgumentException("collectionNamesCacheMaxSize must be set to a positive value");
        }
        this.collectionNamesCacheMaxSize = builder.collectionNamesCacheMaxSize;
        this.collectionNamesCacheOverflowErrorTracker = builder.collectionNamesCacheOverflowErrorLogInterval < 1 ? null : new EveryNthOperation(builder.collectionNamesCacheOverflowErrorLogInterval);
    }

    @Override
    public void commandStarted(CommandStartedEvent event) {
        mongoCommandUtil.determineCollectionName(event.getCommandName(), event.getCommand())
                .ifPresent(collectionName -> addCollectionNameToCache(event, collectionName));
    }

    @Override
    public void commandSucceeded(CommandSucceededEvent event) {
        timeCommand(event, removeCollectionNameFromCache(event), "SUCCESS", event.getElapsedTime(TimeUnit.NANOSECONDS));
    }

    @Override
    public void commandFailed(CommandFailedEvent event) {
        timeCommand(event, removeCollectionNameFromCache(event), "FAILED", event.getElapsedTime(TimeUnit.NANOSECONDS));
    }

    private void timeCommand(CommandEvent event, String collection, String status, long elapsedTimeInNanoseconds) {
        Timer.builder(timerMetricName)
                .description(timerMetricDescription)
                .tag("command", event.getCommandName())
                .tag("cluster.id", event.getConnectionDescription().getConnectionId().getServerId().getClusterId().getValue())
                .tag("server.address", event.getConnectionDescription().getServerAddress().toString())
                .tag("collection", collection)
                .tag("status", status)
                .register(registry)
                .record(elapsedTimeInNanoseconds, TimeUnit.NANOSECONDS);
    }

    private void addCollectionNameToCache(CommandEvent event, String collectionName) {
        if (collectionNamesCache.size() < collectionNamesCacheMaxSize) {
            collectionNamesCache.put(event.getRequestId(), collectionName);
            return;
        }
        if (collectionNamesCacheOverflowErrorTracker == null) {
            return;
        }
        // Cache over capacity - log every ~Nth for a good balance of warning and not spamming the logs.
        collectionNamesCacheOverflowErrorTracker.maybeInvoke(() -> LOG.warn("Collection names cache is full - Mongo is not calling listeners properly"));
    }

    private String removeCollectionNameFromCache(CommandEvent event) {
        return Optional.ofNullable(collectionNamesCache.remove(event.getRequestId()))
                .orElse("unknown");
    }

    public static Builder builder(MeterRegistry registry) {
        return new Builder(registry);
    }

    public static class Builder {
        private final MeterRegistry registry;
        private String timerMetricName = "mongodb.driver.commands";
        private String timerMetricDescription = "Timer of mongodb commands";
        private int collectionNamesCacheMaxSize = 1000;
        private int collectionNamesCacheOverflowErrorLogInterval = 100;

        private Builder(MeterRegistry registry) {
            this.registry = registry;
        }

        public Builder timerMetricName(String timerMetricName) {
            this.timerMetricName = timerMetricName;
            return this;
        }

        public Builder timerMetricDescription(String timerMetricDescription) {
            this.timerMetricDescription = timerMetricDescription;
            return this;
        }

        public Builder collectionNamesCacheMaxSize(int maxSize) {
            this.collectionNamesCacheMaxSize = maxSize;
            return this;
        }

        public Builder collectionNamesCacheOverflowErrorLogInterval(int logInterval) {
            this.collectionNamesCacheOverflowErrorLogInterval = logInterval;
            return this;
        }

        public MongoMetricsCommandListener build() {
            return new MongoMetricsCommandListener(this);
        }
    }
}

