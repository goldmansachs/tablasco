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

package com.gs.tablasco.adapters;

import com.gs.tablasco.VerifiableTable;

import java.util.function.IntPredicate;
import java.util.function.Predicate;

public class TableAdapters
{
    /**
     * Takes a table and row filter and returns an adapted table that contains only rows that match the filter.
     *
     * @param delegate the table to adapt
     * @param rowFilter a predicate that takes row index and table and returns true if the row should be included
     * @return the adapted table
     */
    public static VerifiableTable withRows(VerifiableTable delegate, IntPredicate rowFilter)
    {
        return new RowFilterAdapter(delegate, rowFilter);
    }

    /**
     * Takes a table and column filter and returns an adapted table that contains only columns that match the filter.
     *
     * @param delegate the table to adapt
     * @param columnFilter a predicate that takes row index and table and returns true if the row should be included
     * @return the adapted table
     */
    public static VerifiableTable withColumns(VerifiableTable delegate, Predicate<String> columnFilter)
    {
        return new ColumnFilterAdapter(delegate, columnFilter);
    }

    private TableAdapters()
    {

    }
}
