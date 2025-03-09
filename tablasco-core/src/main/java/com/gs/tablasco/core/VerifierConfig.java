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

package com.gs.tablasco.core;

import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.adapters.TableAdapters;
import com.gs.tablasco.verify.ColumnComparators;
import com.gs.tablasco.verify.indexmap.IndexMapTableVerifier;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

public final class VerifierConfig
{
    private final ColumnComparators.Builder columnComparatorsBuilder = new ColumnComparators.Builder();
    private boolean verifyRowOrder = true;
    private boolean ignoreSurplusRows = false;
    private boolean ignoreMissingRows = false;
    private boolean ignoreSurplusColumns = false;
    private boolean ignoreMissingColumns = false;
    private Function<VerifiableTable, VerifiableTable> actualAdapter = verifiableTable -> verifiableTable;
    private Function<VerifiableTable, VerifiableTable> expectedAdapter = verifiableTable -> verifiableTable;
    private long partialMatchTimeoutMillis = IndexMapTableVerifier.DEFAULT_PARTIAL_MATCH_TIMEOUT_MILLIS;

    /**
     * Returns the same instance of {@link VerifierConfig} configured with row order verification disabled. If this is
     * disabled a test will pass if the cells match but row order is different between actual and expected results.
     *
     * @param verifyRowOrder whether to verify row order or not
     * @return this
     */
    public VerifierConfig withVerifyRowOrder(boolean verifyRowOrder)
    {
        this.verifyRowOrder = verifyRowOrder;
        return this;
    }

    /**
     * Returns the same instance of {@link VerifierConfig} configured with a numeric tolerance to apply when matching
     * floating point numbers.
     * <p>
     * Note: this tolerance applies to all floating-point column types which could be dangerous. It is generally
     * advisable to set tolerance per column using {@link #withTolerance(String, double) withTolerance}
     *
     * @param tolerance the tolerance to apply
     * @return this
     */
    public VerifierConfig withTolerance(double tolerance)
    {
        this.columnComparatorsBuilder.withTolerance(tolerance);
        return this;
    }

    /**
     * Returns the same instance of {@link VerifierConfig} configured with a numeric tolerance to apply when matching
     * floating point numbers for the given column.
     *
     * @param columnName the column name for which the tolerance will be applied
     * @param tolerance the tolerance to apply
     * @return this
     */
    public VerifierConfig withTolerance(String columnName, double tolerance)
    {
        this.columnComparatorsBuilder.withTolerance(columnName, tolerance);
        return this;
    }

    /**
     * Returns the same instance of {@link VerifierConfig} configured with a variance threshold to apply when matching
     * numbers.
     * <p>
     * Note: this variance threshold applies to all floating-point column types which could be dangerous. It is
     * generally advisable to set variance threshold per column using {@link #withVarianceThreshold(String, double)
     * withVarianceThreshold}
     *
     * @param varianceThreshold the variance threshold to apply
     * @return this
     */
    public VerifierConfig withVarianceThreshold(double varianceThreshold)
    {
        this.columnComparatorsBuilder.withVarianceThreshold(varianceThreshold);
        return this;
    }

    /**
     * Returns the same instance of {@link VerifierConfig} configured with a variance threshold to apply when matching
     * numbers for the given column.
     *
     * @param columnName the column name for which the variance will be applied
     * @param varianceThreshold the variance threshold to apply
     * @return this
     */
    public VerifierConfig withVarianceThreshold(String columnName, double varianceThreshold)
    {
        this.columnComparatorsBuilder.withVarianceThreshold(columnName, varianceThreshold);
        return this;
    }

    /**
     * Returns the same instance of {@link VerifierConfig} configured with a function for adapting actual results. Each
     * table in the actual results will be adapted using the specified function before being verified or rebased.
     *
     * @param actualAdapter function for adapting tables
     * @return this
     */
    public VerifierConfig withActualAdapter(Function<VerifiableTable, VerifiableTable> actualAdapter)
    {
        this.actualAdapter = actualAdapter;
        return this;
    }

    /**
     * Returns the actual table adapter
     * @return - the actual table adapter
     */
    public Function<VerifiableTable, VerifiableTable> getActualAdapter()
    {
        return actualAdapter;
    }

    /**
     * Returns the same instance of {@link VerifierConfig} configured with a function for adapting expected results.
     * Each table in the expected results will be adapted using the specified function before being verified.
     *
     * @param expectedAdapter function for adapting tables
     * @return this
     */
    public VerifierConfig withExpectedAdapter(Function<VerifiableTable, VerifiableTable> expectedAdapter)
    {
        this.expectedAdapter = expectedAdapter;
        return this;
    }

    /**
     * Returns the expected table adapter
     * @return - the expected table adapter
     */
    public Function<VerifiableTable, VerifiableTable> getExpectedAdapter()
    {
        return expectedAdapter;
    }

    /**
     * Returns the same instance of {@link VerifierConfig} configured to ignore surplus rows from the verification.
     *
     * @return this
     */
    public VerifierConfig withIgnoreSurplusRows()
    {
        this.ignoreSurplusRows = true;
        return this;
    }

    /**
     * Returns the same instance of {@link VerifierConfig} configured to ignore missing rows from the verification.
     *
     * @return this
     */
    public VerifierConfig withIgnoreMissingRows()
    {
        this.ignoreMissingRows = true;
        return this;
    }

    /**
     * Returns the same instance of {@link VerifierConfig} configured to ignore surplus columns from the verification.
     *
     * @return this
     */
    public VerifierConfig withIgnoreSurplusColumns()
    {
        this.ignoreSurplusColumns = true;
        return this;
    }

    /**
     * Returns the same instance of {@link VerifierConfig} configured to ignore missing columns from the verification.
     *
     * @return this
     */
    public VerifierConfig withIgnoreMissingColumns()
    {
        this.ignoreMissingColumns = true;
        return this;
    }

    /**
     * Returns the same instance of {@link VerifierConfig} configured to ignore columns from both the actual and
     * expected tables.
     *
     * @param columnsToIgnore the columns to ignore
     * @return this
     */
    public VerifierConfig withIgnoreColumns(String... columnsToIgnore)
    {
        final Set<String> columnSet = new HashSet<>(Arrays.asList(columnsToIgnore));
        return this.withColumnFilter(s -> !columnSet.contains(s));
    }

    /**
     * Returns the same instance of {@link VerifierConfig} configured to filter columns from both the actual and
     * expected tables.
     *
     * @param columnFilter the column filter to apply
     * @return this
     */
    public VerifierConfig withColumnFilter(final Predicate<String> columnFilter)
    {
        Function<VerifiableTable, VerifiableTable> adapter = verifiableTable -> TableAdapters.withColumns(verifiableTable, columnFilter);
        return this.withActualAdapter(adapter).withExpectedAdapter(adapter);
    }

    /**
     * Returns the same instance of {@link VerifierConfig} configured with the specified partial match timeout. A value
     * of zero or less results in no timeout.
     *
     * @param partialMatchTimeoutMillis verification timeout in milliseconds
     * @return this
     */
    public VerifierConfig withPartialMatchTimeoutMillis(long partialMatchTimeoutMillis)
    {
        this.partialMatchTimeoutMillis = partialMatchTimeoutMillis;
        return this;
    }

    /**
     * Returns the same instance of {@link VerifierConfig} configured with no partial match timeout.
     *
     * @return this
     */
    public VerifierConfig withoutPartialMatchTimeout()
    {
        return this.withPartialMatchTimeoutMillis(0);
    }

    public ColumnComparators getColumnComparators() {
        return this.columnComparatorsBuilder.build();
    }

    public boolean isVerifyRowOrder() {
        return verifyRowOrder;
    }

    public boolean isIgnoreSurplusRows() {
        return ignoreSurplusRows;
    }

    public boolean isIgnoreMissingRows() {
        return ignoreMissingRows;
    }

    public boolean isIgnoreSurplusColumns() {
        return ignoreSurplusColumns;
    }

    public boolean isIgnoreMissingColumns() {
        return ignoreMissingColumns;
    }

    public long getPartialMatchTimeoutMillis() {
        return partialMatchTimeoutMillis;
    }

}