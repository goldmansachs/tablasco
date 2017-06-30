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

import com.gs.tablasco.VerifiableTable;

import java.util.concurrent.Callable;

/**
 * Represents a level of investigation. The actual and expected tables returned at this level must have the same
 * structure and the last column must be the matching key column.
 */
public interface InvestigationLevel
{
    /**
     * @return a code block for retrieving the actual results table
     */
    Callable<VerifiableTable> getActualResults();

    /**
     * @return a code block for retrieving the expected results
     */
    Callable<VerifiableTable> getExpectedResults();

    /**
     * @return the description of this level
     */
    String getLevelDescription();
}
