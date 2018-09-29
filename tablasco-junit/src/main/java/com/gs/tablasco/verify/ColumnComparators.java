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

import org.eclipse.collections.api.block.function.Function2;
import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.api.map.MapIterable;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.tuple.Twin;
import org.eclipse.collections.impl.factory.Maps;
import org.eclipse.collections.impl.tuple.Tuples;

import java.io.Serializable;

public class ColumnComparators implements Serializable
{
    private final Twin<CellComparator> defaultCellComparator;
    private final MapIterable<String, Twin<CellComparator>> comparatorsByColumn;

    private ColumnComparators(Double defaultTolerance, Double defaultVarianceThreshold, ImmutableMap<String, Builder.ToleranceVarianceValues> values)
    {
        this.defaultCellComparator = Tuples.twin(
                getCellComparator(defaultTolerance, defaultVarianceThreshold),
                new ToleranceCellComparator(getCellFormatter(defaultTolerance, false)));

        this.comparatorsByColumn = values.collectValues(new Function2<String, Builder.ToleranceVarianceValues, Twin<CellComparator>>() {
        @Override
        public Twin<CellComparator> value(String columnName, Builder.ToleranceVarianceValues toleranceVarianceValues) {
            return Tuples.twin(
                    getCellComparator(toleranceVarianceValues.tolerance, toleranceVarianceValues.varianceThreshold),
                    new ToleranceCellComparator(getCellFormatter(toleranceVarianceValues.tolerance, false)));
        }
    });
    }

    public CellComparator getDefaultComparator()
    {
        return this.defaultCellComparator.getOne();
    }

    public CellComparator getComparator(String columnName)
    {
        Twin<CellComparator> cellComparators = this.comparatorsByColumn.get(columnName);
        return cellComparators == null ? getDefaultComparator() : cellComparators.getOne();
    }

    public CellComparator getComparatorForRebase(String columnName)
    {
        Twin<CellComparator> cellComparators = this.comparatorsByColumn.get(columnName);
        return cellComparators == null ? this.defaultCellComparator.getTwo() : cellComparators.getTwo();
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
        private final MutableMap<String, ToleranceVarianceValues> toleranceVarianceValues = Maps.mutable.of();

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
            return new ColumnComparators(this.defaultTolerance, this.defaultVarianceThreshold, this.toleranceVarianceValues.toImmutable());
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
}
