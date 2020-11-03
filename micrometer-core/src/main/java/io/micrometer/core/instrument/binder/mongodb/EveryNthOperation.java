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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Counts occurrences of a situation of interest and invokes an operation every Nth time the situation occurs.
 *
 * <p>A common use is to log a message every N times a situation occurs to avoid spamming the application log.
 *
 * <p>Uses non-blocking concurrency and is thread safe.
 */
class EveryNthOperation {

    private final AtomicInteger callCount = new AtomicInteger(0);
    private final int nth;

    EveryNthOperation(final int nth) {
        if (nth < 1) {
            throw new IllegalArgumentException("nth must be > 0");
        }
        this.nth = nth;
    }

    /**
     * Called to signal the situation of interest has occurred and invoke the specified operation if its the Nth time.
     *
     * @param operation the operation to invoke
     */
    void maybeInvoke(final Runnable operation) {
        // Only one thread will ever inc the Nth-1 number successfully, when that occurs set count to zero and invoke op
        final int valueBeforeUpdate = callCount.getAndUpdate(i -> (i + 1) == nth ? 0 : (i + 1));
        if (valueBeforeUpdate == 0) {
            operation.run();
        }
    }
}
