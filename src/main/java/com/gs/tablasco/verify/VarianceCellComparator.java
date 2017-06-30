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


public class VarianceCellComparator extends CellComparator
{
    private final double varianceThreshold;

    public VarianceCellComparator(CellFormatter formatter, double varianceThreshold)
    {
        super(formatter);
        this.varianceThreshold = varianceThreshold;
    }

    @Override
    public boolean compare(Object actual, Object expected)
    {
        if (isFloatingPoint(expected) && isFloatingPoint(actual))
        {
            double variance = getVariance(actual, expected);
            return Math.abs(variance) <= this.varianceThreshold;
        }
        return false;
    }

    public static double getVariance(Object actual, Object expected)
    {
        double number1 = ((Number) actual).doubleValue();
        double number2 = ((Number) expected).doubleValue();
        return (number1 - number2) * 100.0d / number2;
    }
}
