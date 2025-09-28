/*
 * Copyright 2017 Goldman Sachs.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package com.gs.tablasco.verify.indexmap;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class TimeBoundPartialMatcherTest {
    @Test
    void executionTimesOut() {
        PartialMatcher endlessMatcher = (allMissingRows, allSurplusRows, matchedColumns) -> {
            try {
                Thread.sleep(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("Unexpected interrupt");
            }
        };
        RuntimeException runtimeException = assertThrows(
                RuntimeException.class, () -> new TimeBoundPartialMatcher(endlessMatcher, 1L).match(null, null, null));
        assertInstanceOf(TimeoutException.class, runtimeException.getCause());
    }

    @Test
    void matchingExceptionPropagates() {
        assertThrows(IndexOutOfBoundsException.class, () -> {
            PartialMatcher dyingMatcher = (allMissingRows, allSurplusRows, matchedColumns) -> {
                throw new IndexOutOfBoundsException("Boom");
            };
            new TimeBoundPartialMatcher(dyingMatcher, Long.MAX_VALUE).match(null, null, null);
        });
    }

    @Test
    void matchingErrorPropagates() {
        PartialMatcher dyingMatcher = (allMissingRows, allSurplusRows, matchedColumns) -> {
            throw new NoSuchMethodError();
        };
        RuntimeException runtimeException =
                assertThrows(RuntimeException.class, () -> new TimeBoundPartialMatcher(dyingMatcher, Long.MAX_VALUE)
                        .match(null, null, null));
        assertInstanceOf(NoSuchMethodError.class, runtimeException.getCause());
    }

    @Test
    void successfulMatch() {
        final AtomicBoolean matched = new AtomicBoolean(false);
        PartialMatcher matcher = (allMissingRows, allSurplusRows, matchedColumns) -> matched.set(true);
        new TimeBoundPartialMatcher(matcher, Long.MAX_VALUE).match(null, null, null);
        assertTrue(matched.get());
    }
}
