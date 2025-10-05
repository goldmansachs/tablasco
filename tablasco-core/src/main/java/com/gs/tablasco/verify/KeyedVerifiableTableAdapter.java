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

public class KeyedVerifiableTableAdapter extends DefaultVerifiableTableAdapter implements KeyedVerifiableTable {
    private final boolean[] keyColumnIndices;

    public KeyedVerifiableTableAdapter(VerifiableTable delegate, int... keyColumnIndices) {
        super(delegate);
        this.keyColumnIndices = new boolean[delegate.getColumnCount()];
        for (int keyColumnIndex : keyColumnIndices) {
            this.keyColumnIndices[keyColumnIndex] = true;
        }
    }

    @Override
    public boolean isKeyColumn(int columnIndex) {
        return columnIndex >= 0 && columnIndex < this.keyColumnIndices.length && this.keyColumnIndices[columnIndex];
    }
}
