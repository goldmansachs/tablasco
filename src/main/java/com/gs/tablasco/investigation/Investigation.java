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

import java.util.List;

/**
 * Defines a series of investigation levels that verifier can execute, compare and format.
 */
public interface Investigation
{
    /**
     * @return the first level of investigation
     */
    InvestigationLevel getFirstLevel();

    /**
     * @param drilldownKeys the keys of rows that did not match in the previous level of investigation
     * @return the next level of investigation or null if the investigation is complete.
     */
    InvestigationLevel getNextLevel(List<Object> drilldownKeys);

    /**
     * @return the maximum number of row keys to process at each level of investigation
     */
    int getRowKeyLimit();
}
