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

import java.io.Serializable;

public class ToleranceCellComparator extends CellComparator implements Serializable
{
    public ToleranceCellComparator(CellFormatter formatter)
    {
        super(formatter);
    }

    @Override
    public boolean compare(Object actual, Object expected)
    {
        if (isFloatingPoint(expected) && isFloatingPoint(actual))
        {
            double actualVal = ((Number) actual).doubleValue();
            double expectVal = ((Number) expected).doubleValue();
            return Double.compare(expectVal, actualVal) == 0 || Math.abs(expectVal - actualVal) <= getFormatter().getTolerance();
        }
        return false;
    }

    static double getDifference(Object actual, Object expected)
    {
        return ((Number) expected).doubleValue() - ((Number) actual).doubleValue();
    }
}
