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

import com.gs.tablasco.VerifiableTable;

/**
 * A default {@link VerifiableTable} adapter that delegates all calls to an underlying delegate table. Extend this
 * class if you only need to modify behaviour of some methods of the udnerlying table.
 */
public abstract class DefaultVerifiableTableAdapter implements VerifiableTable
{
    private final VerifiableTable delegate;

    /**
     * Creates a new {@link DefaultVerifiableTableAdapter} with an underlying table to which calls should be delegated.
     * @param delegate underlying table to which calls should be delegated
     */
    protected DefaultVerifiableTableAdapter(VerifiableTable delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public int getRowCount()
    {
        return this.delegate.getRowCount();
    }

    @Override
    public int getColumnCount()
    {
        return this.delegate.getColumnCount();
    }

    @Override
    public String getColumnName(int columnIndex)
    {
        return this.delegate.getColumnName(columnIndex);
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex)
    {
        return this.delegate.getValueAt(rowIndex, columnIndex);
    }
}
