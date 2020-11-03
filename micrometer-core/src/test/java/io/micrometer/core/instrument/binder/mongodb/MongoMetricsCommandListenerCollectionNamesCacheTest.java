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

import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit test for {@link MongoMetricsCommandListener} collection name cache aspects - ensures its bounded and when full
 * results in unknown collection name.
 */
class MongoMetricsCommandListenerCollectionNamesCacheTest {

    private final ConnectionDescription connectionDesc = new ConnectionDescription(
            new ServerId(new ClusterId("cluster1"), new ServerAddress("localhost", 5150)));

    @Test
    void doesNotAllowNonPositiveMaxCacheSizeParameter() {
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        assertThatThrownBy(() -> MongoMetricsCommandListener.builder(meterRegistry).collectionNamesCacheMaxSize(0).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("collectionNamesCacheMaxSize must be set to a positive value");
    }

    @Test
    void collectionNamesCacheOverflowErrorTrackerDisabledWhenIntervalSetToZero() {
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        assertThat(MongoMetricsCommandListener.builder(meterRegistry).collectionNamesCacheOverflowErrorLogInterval(0).build().collectionNamesCacheOverflowErrorTracker).isNull();
    }

    @Test
    void collectionNamesCacheOverflowErrorTrackerDisabledWhenIntervalSetToNegative() {
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        assertThat(MongoMetricsCommandListener.builder(meterRegistry).collectionNamesCacheOverflowErrorLogInterval(-1).build().collectionNamesCacheOverflowErrorTracker).isNull();
    }

    @Test
    void collectionNamesCacheOverflowErrorTrackerCreatedWhenIntervalIsPositive() {
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        assertThat(MongoMetricsCommandListener.builder(meterRegistry).collectionNamesCacheOverflowErrorLogInterval(25).build().collectionNamesCacheOverflowErrorTracker).isNotNull();
    }

    @Test
    void handlesCacheFullGracefully() {
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        MongoMetricsCommandListener listener = MongoMetricsCommandListener.builder(meterRegistry).collectionNamesCacheMaxSize(1000).build();
        for (int i = 1; i <= 1000; i++) {
            listener.commandStarted(commandStartedEvent(i));
        }
        // At this point we have put 1000 into the starting state (and cache) but no more will be put into cache

        // 1001 will not be added to cache and therefore will use 'unknown'
        listener.commandStarted(commandStartedEvent(1001));
        listener.commandSucceeded(commandSucceededEvent(1001));
        assertHasMetricWithCollectionName(meterRegistry, "unknown");

        // Complete 1000 - which will remove previously added entry from cache
        listener.commandSucceeded(commandSucceededEvent(1000));
        assertHasMetricWithCollectionName(meterRegistry,"collection-1000");

        // 1001 will now be put in cache (since 1000 removed and made room for it)
        listener.commandStarted(commandStartedEvent(1001));
        listener.commandSucceeded(commandSucceededEvent(1001));
        assertHasMetricWithCollectionName(meterRegistry, "collection-1001");

        // 1002 will not be added to cache and therefore will use 'unknown'
        listener.commandStarted(commandStartedEvent(1002));
        listener.commandSucceeded(commandSucceededEvent(1002));
        assertHasMetricWithCollectionName(meterRegistry, "unknown");
    }

    @Test
    void invokesEveryNthOperationOnCacheOverflow() {
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        MongoMetricsCommandListener listener = MongoMetricsCommandListener.builder(meterRegistry).build();
        for (int i = 1; i <= 1000; i++) {
            listener.commandStarted(commandStartedEvent(i));
        }
        // At this point we have put 1000 into the starting state (and cache) but no more will be put into cache

        // 1001 will not be added to cache and therefore will use 'unknown'
        listener.commandStarted(commandStartedEvent(1001));
        listener.commandSucceeded(commandSucceededEvent(1001));
        assertHasMetricWithCollectionName(meterRegistry, "unknown");

        // Complete 1000 - which will remove previously added entry from cache
        listener.commandSucceeded(commandSucceededEvent(1000));
        assertHasMetricWithCollectionName(meterRegistry,"collection-1000");

        // 1001 will now be put in cache (since 1000 removed and made room for it)
        listener.commandStarted(commandStartedEvent(1001));
        listener.commandSucceeded(commandSucceededEvent(1001));
        assertHasMetricWithCollectionName(meterRegistry, "collection-1001");
    }

    private CommandStartedEvent commandStartedEvent(final int requestId) {
        return new CommandStartedEvent(
                requestId,
                connectionDesc,
                "db1",
                "find",
                new BsonDocument("find", new BsonString("collection-" + requestId)));
    }

    private CommandSucceededEvent commandSucceededEvent(final int requestId) {
        return new CommandSucceededEvent(
                requestId,
                connectionDesc,
                "find",
                new BsonDocument(),
                1200L);
    }

    private void assertHasMetricWithCollectionName(final MeterRegistry meterRegistry, final String collectionName) {
        assertThat(meterRegistry.get("mongodb.driver.commands").timers())
                .anySatisfy(timer -> assertThat(timer.getId().getTag("collection")).isNotEmpty().isEqualTo(collectionName));
    }
}