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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ColumnComparators implements Serializable
{
    private final CellComparatorPair defaultCellComparator;
    private final Map<String, CellComparatorPair> comparatorsByColumn;

    private ColumnComparators(Double defaultTolerance, Double defaultVarianceThreshold, Map<String, Builder.ToleranceVarianceValues> values)
    {
        this.defaultCellComparator = new CellComparatorPair(
                getCellComparator(defaultTolerance, defaultVarianceThreshold),
                new ToleranceCellComparator(getCellFormatter(defaultTolerance, false)));

        this.comparatorsByColumn = values.keySet().stream().collect(Collectors.toMap(key -> key, key -> {
            Builder.ToleranceVarianceValues toleranceVarianceValues = values.get(key);
            return new CellComparatorPair(
                    getCellComparator(toleranceVarianceValues.tolerance, toleranceVarianceValues.varianceThreshold),
                    new ToleranceCellComparator(getCellFormatter(toleranceVarianceValues.tolerance, false)));
        }));
    }

    public CellComparator getDefaultComparator()
    {
        return this.defaultCellComparator.getRunComparator();
    }

    public CellComparator getComparator(String columnName)
    {
        CellComparatorPair cellComparators = this.comparatorsByColumn.get(columnName);
        return cellComparators == null ? getDefaultComparator() : cellComparators.getRunComparator();
    }

    public CellComparator getComparatorForRebase(String columnName)
    {
        CellComparatorPair cellComparators = this.comparatorsByColumn.get(columnName);
        return cellComparators == null ? this.defaultCellComparator.getRebaseComparator() : cellComparators.getRebaseComparator();
    }

    private CellComparator getCellComparator(Double tolerance, Double varianceThreshold)
    {
        CellFormatter cellFormatter = getCellFormatter(tolerance, true);
        if (varianceThreshold == null)
        {
            return new ToleranceCellComparator(cellFormatter);
        }
        else
        {
            return new VarianceCellComparator(cellFormatter, varianceThreshold);
        }
    }

    private CellFormatter getCellFormatter(Double tolerance, boolean isGroupingUsed)
    {
        return new CellFormatter(tolerance == null ? 1.0e-14d : tolerance, isGroupingUsed);
    }

    public static class Builder
    {
        private Double defaultTolerance;
        private Double defaultVarianceThreshold;
        private final Map<String, ToleranceVarianceValues> toleranceVarianceValues = new HashMap<>();

        public Builder withTolerance(double tolerance)
        {
            this.defaultTolerance = tolerance;
            return this;
        }

        public Builder withTolerance(String columnName, double tolerance)
        {
            this.toleranceVarianceValues.put(columnName, toleranceVarianceValue(columnName).withTolerance(tolerance));
            return this;
        }

        public Builder withVarianceThreshold(double varianceThreshold)
        {
            this.defaultVarianceThreshold = varianceThreshold;
            return this;
        }

        public Builder withVarianceThreshold(String columnName, double varianceThreshold)
        {
            this.toleranceVarianceValues.put(columnName, toleranceVarianceValue(columnName).withVarianceThreshold(varianceThreshold));
            return this;
        }

        public ColumnComparators build()
        {
            return new ColumnComparators(this.defaultTolerance, this.defaultVarianceThreshold, Collections.unmodifiableMap(this.toleranceVarianceValues));
        }

        private ToleranceVarianceValues toleranceVarianceValue(String columnName) {
            return this.toleranceVarianceValues.get(columnName) == null
                    ? new ToleranceVarianceValues() : this.toleranceVarianceValues.get(columnName);
        }

        private static class ToleranceVarianceValues
        {
            private Double tolerance;
            private Double varianceThreshold;

            public ToleranceVarianceValues withTolerance(Double tolerance) {
                this.tolerance = tolerance;
                return this;
            }

            public ToleranceVarianceValues withVarianceThreshold(Double varianceThreshold) {
                this.varianceThreshold = varianceThreshold;
                return this;
            }
        }
    }

    public static class CellComparatorPair implements Serializable
    {
        private final CellComparator runComparator;
        private final CellComparator rebaseComparator;

        CellComparatorPair(CellComparator runComparator, CellComparator rebaseComparator)
        {
            this.runComparator = runComparator;
            this.rebaseComparator = rebaseComparator;
        }

        CellComparator getRunComparator()
        {
            return runComparator;
        }

        CellComparator getRebaseComparator()
        {
            return rebaseComparator;
        }
    }
}
