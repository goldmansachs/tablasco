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

package com.gs.tablasco.rebase;

import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.verify.ColumnComparators;
import com.gs.tablasco.verify.Metadata;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.File;
import java.util.Map;

public class Rebaser
{
    private static final Logger LOGGER = Logger.getLogger(Rebaser.class.getSimpleName());
    private static Boolean rebaseMode;
    private final ColumnComparators columnComparators;
    private final Metadata metadata;
    private final String[] baselineHeaders;

    public static boolean inRebaseMode()
    {
        if (rebaseMode == null)
        {
            rebaseMode = initializeRebaseFlag();
        }
        return rebaseMode;
    }

    public Rebaser(ColumnComparators columnComparators, Metadata metadata, String[] baselineHeaders)
    {
        this.columnComparators = columnComparators;
        this.metadata = metadata;
        this.baselineHeaders = baselineHeaders;
    }

    public void rebase(String methodName, Map<String, VerifiableTable> actualResults, File outputFile)
    {
        new RebaseFileWriter(this.metadata, this.baselineHeaders, this.columnComparators, outputFile).writeRebasedResults(methodName, actualResults);
    }

    private static Boolean initializeRebaseFlag()
    {
        Boolean rebaseEnabled = Boolean.valueOf(System.getProperty("rebase", "false"));
        if (rebaseEnabled)
        {
            LOGGER.log(Level.WARNING, "Stand back from the platform edge - here comes the");
            LOGGER.log(Level.WARNING, "        ___    ___    ___    ___    ___    ___   _  _    ___    ");
            LOGGER.log(Level.WARNING, "       | _ \\  | __|  | _ )  /   \\  / __|  |_ _| | \\| |  / __|");
            LOGGER.log(Level.WARNING, "       |   /  | _|   | _ \\  | - |  \\__ \\   | |  | .` | | (_ |");
            LOGGER.log(Level.WARNING, "       |_|_\\  |___|  |___/  |_|_|  |___/  |___| |_|\\_|  \\___|");
            LOGGER.log(Level.WARNING, "     _|\"\"\"\"\"||\"\"\"\"\"||\"\"\"\"\"||\"\"\"\"\"||\"\"\"\"\"||\"\"\"\"\"||\"\"\"\"\"||\"\"\"\"\"|");
            LOGGER.log(Level.WARNING, "      `-0-0-'`-0-0-'`-0-0-'`-0-0-'`-0-0-'`-0-0-'`-0-0-'`-0-0-'");
            LOGGER.log(Level.WARNING, "train.... ");
        }
        return rebaseEnabled;
    }
}