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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Unit tests for {@link EveryNthOperation}.
 */
class EveryNthOperationTest {

    @ParameterizedTest
    @MethodSource("singleThreadOperationsProvider")
    void singleThreadOperations(final int nth, final int numOccurrences, final int expectedInvocationCount) {
        final EveryNthOperation everyNthOperation = new EveryNthOperation(nth);
        final AtomicInteger invocations = new AtomicInteger(0);
        for (int i = 0; i < numOccurrences; i++) {
            everyNthOperation.maybeInvoke(invocations::incrementAndGet);
        }
        assertThat(invocations).hasValue(expectedInvocationCount);
    }

    static Stream<Arguments> singleThreadOperationsProvider() {
        return Stream.of(
                arguments(10, 2, 1),
                arguments(10, 10, 1),
                arguments(10, 11, 2),
                arguments(10, 20, 2),
                arguments(10, 21, 3),
                arguments(10, 90, 9),
                arguments(10, 91, 10),
                arguments(10, 100, 10),
                arguments(10, 101, 11),
                arguments(1, 100, 100),
                arguments(100, 100, 1),
                arguments(100, 101, 2)
        );
    }

    @Test
    void multipleThreadOperations() throws ExecutionException, InterruptedException {
        final EveryNthOperation everyNthOperation = new EveryNthOperation(3);
        final AtomicInteger invocations = new AtomicInteger(0);
        CompletableFuture.allOf(IntStream.range(0, 3)
                .mapToObj(i -> CompletableFuture.runAsync(() -> {
                    for (int j = 0; j < 9; j++) {
                        randomSleep();
                        everyNthOperation.maybeInvoke(invocations::incrementAndGet);
                    }
                })).toArray(CompletableFuture[]::new)).get();
        assertThat(invocations).hasValue(9);
    }

    private void randomSleep() {
        try {
            Thread.sleep((long) (Math.random() * 500));
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }
}
