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

package com.gs.tablasco.verify;

import java.util.Set;

class HtmlOptions
{
    private final Set<String> tablesToHideMatchedRows;
    private final boolean displayAssertionSummary;
    private final boolean hideMatchedColumns;
    private final int htmlRowLimit;

    HtmlOptions(Set<String> tablesToHideMatchedRows, boolean displayAssertionSummary, boolean hideMatchedColumns, int htmlRowLimit)
    {

        this.tablesToHideMatchedRows = tablesToHideMatchedRows;
        this.displayAssertionSummary = displayAssertionSummary;
        this.hideMatchedColumns = hideMatchedColumns;
        this.htmlRowLimit = htmlRowLimit;
    }

    boolean isHideMatchedColumns()
    {
        return this.hideMatchedColumns;
    }

    boolean isDisplayAssertionSummary()
    {
        return this.displayAssertionSummary;
    }

    int getHtmlRowLimit()
    {
        return this.htmlRowLimit;
    }

    boolean isHideMatchedRowsFor(String tableName)
    {
        return this.tablesToHideMatchedRows.contains(tableName);
    }
}
