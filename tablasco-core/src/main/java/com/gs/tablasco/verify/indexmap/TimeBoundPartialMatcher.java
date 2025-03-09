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

import java.util.List;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeBoundPartialMatcher implements PartialMatcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeBoundPartialMatcher.class);

    private final PartialMatcher delegate;
    private final long timeoutMillis;

    TimeBoundPartialMatcher(PartialMatcher delegate, long timeoutMillis) {
        this.delegate = delegate;
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public void match(
            final List<UnmatchedIndexMap> allMissingRows,
            final List<UnmatchedIndexMap> allSurplusRows,
            final List<IndexMap> matchedColumns) {
        LOGGER.debug("Starting partial match");
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> result = executorService.submit(
                () -> TimeBoundPartialMatcher.this.delegate.match(allMissingRows, allSurplusRows, matchedColumns));
        try {
            result.get(this.timeoutMillis, TimeUnit.MILLISECONDS);
            LOGGER.debug("Partial match complete");
        } catch (InterruptedException e) {
            LOGGER.error("Partial match interrupted", e);
        } catch (ExecutionException e) {
            LOGGER.error("Partial match exception", e);
            Throwable cause = e.getCause();
            throw cause instanceof RuntimeException ? (RuntimeException) cause : new RuntimeException(cause);
        } catch (TimeoutException e) {
            LOGGER.error("Partial match timed out");
            throw new RuntimeException(e);
        } finally {
            if (!executorService.isTerminated()) {
                executorService.shutdownNow();
            }
        }
    }
}
