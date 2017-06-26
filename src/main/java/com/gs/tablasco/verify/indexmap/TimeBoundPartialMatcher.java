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

import org.eclipse.collections.api.list.MutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TimeBoundPartialMatcher implements PartialMatcher
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeBoundPartialMatcher.class);

    private final PartialMatcher delegate;
    private final long timeoutMillis;

    public TimeBoundPartialMatcher(PartialMatcher delegate, long timeoutMillis)
    {
        this.delegate = delegate;
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public void match(final MutableList<UnmatchedIndexMap> allMissingRows, final MutableList<UnmatchedIndexMap> allSurplusRows, final MutableList<IndexMap> matchedColumns)
    {
        LOGGER.debug("Starting partial match");
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> result = executorService.submit(new Runnable()
        {
            @Override
            public void run()
            {
                TimeBoundPartialMatcher.this.delegate.match(allMissingRows, allSurplusRows, matchedColumns);
            }
        });
        try
        {
            result.get(this.timeoutMillis, TimeUnit.MILLISECONDS);
            LOGGER.debug("Partial match complete");
        }
        catch (InterruptedException e)
        {
            LOGGER.error("Partial match interrupted", e);
        }
        catch (ExecutionException e)
        {
            LOGGER.error("Partial match exception", e);
            Throwable cause = e.getCause();
            throw cause instanceof RuntimeException ? (RuntimeException) cause : new RuntimeException(cause);
        }
        catch (TimeoutException e)
        {
            LOGGER.error("Partial match timed out");
            throw new RuntimeException(e);
        }
        finally
        {
            if (!executorService.isTerminated())
            {
                executorService.shutdownNow();
            }
        }
    }
}
