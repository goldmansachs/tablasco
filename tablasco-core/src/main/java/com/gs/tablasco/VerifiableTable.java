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

package com.gs.tablasco;

/**
 * The table model that {@link com.gs.tablasco.verify.SingleTableVerifier} is able to verify. Data structures must be adapted to instances of
 * {@link VerifiableTable} in order to be verified.
 */
public interface VerifiableTable
{
    /**
     * Returns the number of data rows in this table. This does not include column headers.
     * @return the row count
     */
    int getRowCount();

    /**
     * Returns the number of columns in this table.
     * @return the column count
     */
    int getColumnCount();

    /**
     * Returns the column name at a given index.
     * @param columnIndex zero-based column index
     * @return the column name at given index
     */
    String getColumnName(int columnIndex);

    /**
     * Returns the value at a given row and column index
     * @param rowIndex zero-based row index
     * @param columnIndex zero-based column index
     * @return the value at given row and column index
     */
    Object getValueAt(int rowIndex, int columnIndex);
}
