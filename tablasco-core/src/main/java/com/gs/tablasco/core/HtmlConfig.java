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

package com.gs.tablasco.core;

import com.gs.tablasco.verify.HtmlFormatter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public final class HtmlConfig
{
    private boolean hideMatchedRows = false;
    private boolean hideMatchedTables = false;
    private boolean hideMatchedColumns = false;
    private boolean showAssertionSummary = false;
    private final Set<String> tablesToAlwaysShowMatchedRowsFor = new HashSet<>();
    private int htmlRowLimit = HtmlFormatter.DEFAULT_ROW_LIMIT;

    /**
     * Returns the same instance of {@link HtmlConfig} configured to exclude matched rows from the verification
     * output.
     *
     * @param hideMatchedRows whether to hide matched rows or not
     * @return this
     */
    public HtmlConfig withHideMatchedRows(boolean hideMatchedRows)
    {
        this.hideMatchedRows = hideMatchedRows;
        return this;
    }

    /**
     * Returns the same instance of {@link HtmlConfig} configured to always show matched rows for the specified
     * tables. This only makes sense when withHideMatchedRows is true.
     *
     * @param tableNames varargs of table names to always show matched rows for
     * @return this
     */
    public HtmlConfig withAlwaysShowMatchedRowsFor(String... tableNames)
    {
        this.tablesToAlwaysShowMatchedRowsFor.addAll(Arrays.asList(tableNames));
        return this;
    }

    /**
     * Returns the same instance of {@link HtmlConfig} configured to exclude matched columns from the verification
     * output.
     *
     * @param hideMatchedColumns whether to hide matched columns or not
     * @return this
     */
    public HtmlConfig withHideMatchedColumns(boolean hideMatchedColumns)
    {
        this.hideMatchedColumns = hideMatchedColumns;
        return this;
    }

    /**
     * Returns the same instance of {@link HtmlConfig} configured to exclude matched tables from the verification
     * output. If this is enabled and all tables are matched not output file will be created.
     *
     * @param hideMatchedTables whether to hide matched tables or not
     * @return this
     */
    public HtmlConfig withHideMatchedTables(boolean hideMatchedTables)
    {
        this.hideMatchedTables = hideMatchedTables;
        return this;
    }

    /**
     * Returns the same instance of {@link HtmlConfig} configured to limit the number of HTML rows to the specified
     * number.
     *
     * @param htmlRowLimit the number of rows to limit output to
     * @return this
     */
    public HtmlConfig withHtmlRowLimit(int htmlRowLimit)
    {
        this.htmlRowLimit = htmlRowLimit;
        return this;
    }

    /**
     * Adds an assertion summary to html output
     *
     * @return this
     */
    public HtmlConfig withAssertionSummary(boolean assertionSummary)
    {
        this.showAssertionSummary = assertionSummary;
        return this;
    }

    public boolean isHideMatchedRows() {
        return hideMatchedRows;
    }

    public boolean isHideMatchedTables() {
        return hideMatchedTables;
    }

    public boolean isHideMatchedColumns() {
        return hideMatchedColumns;
    }

    public boolean isShowAssertionSummary() {
        return showAssertionSummary;
    }

    public Set<String> getTablesToAlwaysShowMatchedRowsFor() {
        return tablesToAlwaysShowMatchedRowsFor;
    }

    public int getHtmlRowLimit() {
        return htmlRowLimit;
    }
}