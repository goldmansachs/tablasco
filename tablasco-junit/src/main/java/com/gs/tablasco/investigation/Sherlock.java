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

package com.gs.tablasco.investigation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * Compares results from two environments and drills down on breaks in multiple
 * steps until it finds the underlying data responsible for the breaks.
 */
public class Sherlock
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Sherlock.class);

    public void handle(Investigation investigation, File outputFile)
    {
        Watson watson = new Watson(outputFile);
        InvestigationLevel currentLevel = investigation.getFirstLevel();
        List<Object> drilldownKeys = watson.assist("Initial Results", currentLevel, investigation.getRowKeyLimit());
        if (drilldownKeys == null || drilldownKeys.isEmpty())
        {
            LOGGER.info("No breaks found :)");
            return;
        }

        LOGGER.info("Got {} broken drilldown keys - {}", drilldownKeys.size(), outputFile);
        int level = 1;
        while (!drilldownKeys.isEmpty() && (currentLevel = investigation.getNextLevel(drilldownKeys)) != null)
        {
            drilldownKeys = watson.assist("Investigation Level " + level + " (Top " + investigation.getRowKeyLimit() + ')', currentLevel, investigation.getRowKeyLimit());
            LOGGER.info("Got {} broken drilldown keys - {}", drilldownKeys.size(), outputFile);
            level++;
        }

        String message = "Some tests failed. Check test results file " + outputFile.getAbsolutePath() + " for more details.";
        throw new AssertionError(message);
    }
}
