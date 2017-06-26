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

import org.eclipse.collections.api.block.HashingStrategy;

public abstract class CellComparator implements HashingStrategy
{
    private final CellFormatter formatter;

    public CellComparator(CellFormatter formatter)
    {
        this.formatter = formatter;
    }

    public CellFormatter getFormatter()
    {
        return this.formatter;
    }

    protected abstract boolean compare(Object actual, Object expected);

    public static boolean isFloatingPoint(Object object)
    {
        return object instanceof Double || object instanceof Float;
    }

    @Override
    public boolean equals(Object actual, Object expected)
    {
        String formattedActual = this.getFormatter().format(actual);
        String formattedExpected = this.getFormatter().format(expected);
        return formattedActual.equals(formattedExpected) || compare(actual, expected);
    }

    @Override
    public int computeHashCode(Object object)
    {
        return this.formatter.format(object).hashCode();
    }
}
