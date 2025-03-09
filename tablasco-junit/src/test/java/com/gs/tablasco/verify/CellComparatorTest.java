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

import org.junit.Assert;
import org.junit.Test;

public class CellComparatorTest {

    private final double tolerance = 0.1d;
    private final double varianceThreshold = 5.0d;

    public final VarianceCellComparator varianceCellComparator =
            new VarianceCellComparator(new CellFormatter(tolerance, true), varianceThreshold);
    public final ToleranceCellComparator toleranceCellComparator =
            new ToleranceCellComparator(new CellFormatter(tolerance, true));

    @Test
    public void testCompareToleranceStringInputsFail() {
        // String inputs not allowed to tolerance comparator
        Assert.assertFalse(this.varianceCellComparator.compare("ActualAndExpected", "ActualAndExpected"));
        Assert.assertFalse(this.varianceCellComparator.compare("Actual", "Expected"));
    }

    @Test
    public void testCompareNumbersWithinTolerance() {
        Assert.assertTrue(
                "Results match expected. Actual(20.0) and Expected(20.09) within Tolerance range(0.1).",
                this.toleranceCellComparator.compare(20.0, 20.09));
        Assert.assertTrue(
                "Results match expected. Actual(7894.87) and Expected(7894.79) within Tolerance range(0.1).",
                this.toleranceCellComparator.compare(7894.87F, 7894.79F));
    }

    @Test
    public void testCompareNumbersOutsideTolerance() {
        Assert.assertFalse(
                "Results mismatch expected. Actual(20.0) and Expected(20.11) outside Tolerance range(0.1). ",
                this.toleranceCellComparator.compare(20.0, 20.11));
        Assert.assertFalse(
                "Results mismatch expected. Actual(7894.87) and Expected(7894.75) outside Tolerance range(0.1).  ",
                this.toleranceCellComparator.compare(7894.87F, 7894.75F));
    }

    @Test
    public void testCompareVarianceStringInputsFail() {
        // String inputs not allowed to variance comparator
        Assert.assertFalse(this.varianceCellComparator.compare("ActualAndExpected", "ActualAndExpected"));
        Assert.assertFalse(this.varianceCellComparator.compare("Actual", "Expected"));
    }

    @Test
    public void testCompareNumbersWithinVariance() {
        Assert.assertTrue(
                "Results match expected. Actual(2000.0) and Expected(2100.0) within Variance range(5%).",
                this.varianceCellComparator.compare(2000.0, 2100.0));
        Assert.assertTrue(
                "Results match expected. Actual(735.0) and Expected(772.0) within Variance range(5%).",
                this.varianceCellComparator.compare(735.0F, 772.0F));
    }

    @Test
    public void testCompareNumbersOutsideVariance() {
        Assert.assertFalse(
                "Results mismatch expected. Actual(2000.0) and Expected(2110.0) outside Variance range(5%).",
                this.varianceCellComparator.compare(2000.0, 2110.0));
        Assert.assertFalse(
                "Results mismatch expected. Actual(735.0) and Expected(775.0) outside Variance range(5%).",
                this.varianceCellComparator.compare(735.0F, 775.0F));
        Assert.assertFalse(
                "Results mismatch expected. Actual(5600.0) and Expected(NaN) outside Variance range(5%).",
                this.varianceCellComparator.compare(5600.0, Double.NaN));
        Assert.assertFalse(
                "Results mismatch expected. Actual(NaN) and Expected(88.0) outside Variance range(5%).",
                this.varianceCellComparator.compare(Double.NaN, 88.0));
    }

    @Test
    public void testActualExpectedMismatchedTypes() {
        Assert.assertFalse(
                "Results mismatch expected. Actual(390.0) and Expected(expected) outside Variance range(5%).",
                this.varianceCellComparator.compare(390.0, "expected"));
        Assert.assertFalse(
                "Results mismatch expected. Actual(actual) and Expected(1045.0) outside Variance range(5%).",
                this.varianceCellComparator.compare("actual", 1045.0));
    }
}
