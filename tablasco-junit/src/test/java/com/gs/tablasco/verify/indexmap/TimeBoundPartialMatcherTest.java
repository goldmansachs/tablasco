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

import java.util.Collections;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Test;

public class TimeBoundPartialMatcherTest {
    @Test
    public void executionTimesOut() {
        try {
            PartialMatcher endlessMatcher = (allMissingRows, allSurplusRows, matchedColumns) -> {
                try {
                    Thread.sleep(10_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    Assert.fail("Unexpected interrupt");
                }
            };
            new TimeBoundPartialMatcher(endlessMatcher, 1L).match(null, null, null);
            Assert.fail("timeout expected");
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getCause() instanceof TimeoutException);
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void matchingExceptionPropagates() {
        PartialMatcher dyingMatcher = (allMissingRows, allSurplusRows, matchedColumns) ->
                Collections.singletonList("foo").get(2);
        new TimeBoundPartialMatcher(dyingMatcher, Long.MAX_VALUE).match(null, null, null);
    }

    @Test
    public void matchingErrorPropagates() {
        PartialMatcher dyingMatcher = (allMissingRows, allSurplusRows, matchedColumns) -> {
            throw new NoSuchMethodError();
        };
        try {
            new TimeBoundPartialMatcher(dyingMatcher, Long.MAX_VALUE).match(null, null, null);
            Assert.fail();
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getCause() instanceof NoSuchMethodError);
        }
    }

    @Test
    public void successfulMatch() {
        final AtomicBoolean matched = new AtomicBoolean(false);
        PartialMatcher matcher = (allMissingRows, allSurplusRows, matchedColumns) -> matched.set(true);
        new TimeBoundPartialMatcher(matcher, Long.MAX_VALUE).match(null, null, null);
        Assert.assertTrue(matched.get());
    }
}
