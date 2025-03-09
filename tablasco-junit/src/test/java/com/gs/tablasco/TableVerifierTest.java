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

import com.gs.tablasco.lifecycle.ExceptionHandler;
import com.gs.tablasco.lifecycle.LifecycleEventHandler;
import com.gs.tablasco.verify.DefaultVerifiableTableAdapter;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Description;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class TableVerifierTest
{
    private final TableVerifier verifier = new TableVerifier()
            .withExpectedDir(TableTestUtils.getExpectedDirectory())
            .withOutputDir(TableTestUtils.getOutputDirectory())
            .withFilePerClass();

    @Rule
    public final TableTestUtils.TestDescription description = new TableTestUtils.TestDescription();

    @Test
    public void validationSuccess()
    {
        this.verifier.starting(this.description.get());
        this.verifier.verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
        this.verifier.succeeded(this.description.get());
    }

    @Test(expected = AssertionError.class)
    public void validationFailure()
    {
        this.verifier.starting(this.description.get());
        this.verifier.verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
    }

    @Test
    public void toleranceSuccess()
    {
        this.verifier.starting(this.description.get());
        this.verifier.withTolerance(0.2d).withVerifyRowOrder(true).verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
        this.verifier.succeeded(this.description.get());
    }

    @Test
    public void toleranceSuccessForFirstColumn()
    {
        this.verifier.starting(this.description.get());
        this.verifier.withTolerance("Age", 0.2d).verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL_3);
        this.verifier.succeeded(this.description.get());
    }

    @Test
    public void toleranceSuccessForSecondColumn()
    {
        this.verifier.starting(this.description.get());
        this.verifier.withTolerance("Weight", 0.06d).verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL_3);
        this.verifier.succeeded(this.description.get());
    }

    @Test
    public void toleranceSuccessForTwoColumns()
    {
        this.verifier.starting(this.description.get());
        this.verifier.withTolerance("Weight", 0.06d).withTolerance("Age", 0.2d).verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL_3);
        this.verifier.succeeded(this.description.get());
    }

    @Test
    public void toleranceSuccessWithGeneralCase()
    {
        this.verifier.starting(this.description.get());
        this.verifier.withTolerance("Weight", 0.06d).withTolerance(1.0d).verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL_3);
        this.verifier.succeeded(this.description.get());
    }

    @Test(expected = AssertionError.class)
    public void toleranceFailure()
    {
        this.verifier.starting(this.description.get());
        this.verifier.withTolerance(0.1d).withVerifyRowOrder(true).verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
    }

    @Test(expected = AssertionError.class)
    public void toleranceFailureForTwoColumns()
    {
        this.verifier.starting(this.description.get());
        this.verifier.withTolerance("Age", 0.2d).withTolerance("Weight", 0.06d).withVerifyRowOrder(true).verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL_3);
    }

    @Test(expected = AssertionError.class)
    public void toleranceFailureWithGeneralCase()
    {
        this.verifier.starting(this.description.get());
        this.verifier.withTolerance("Weight", 0.06d).withTolerance(1.0d).verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL_3);
    }

    @Test
    public void varianceSuccess()
    {
        this.verifier.starting(this.description.get());
        this.verifier.withVarianceThreshold(5.0d).withVerifyRowOrder(true).verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
        this.verifier.succeeded(this.description.get());
    }

    @Test
    public void varianceSuccessForTwoColumns()
    {
        this.verifier.starting(this.description.get());
        this.verifier.withVarianceThreshold("Weight", 1.0d).withVarianceThreshold("Age", 5.0d).withVerifyRowOrder(true).verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL_3);
        this.verifier.succeeded(this.description.get());
    }

    @Test
    public void varianceSuccessWithTolerance()
    {
        this.verifier.starting(this.description.get());
        this.verifier.withVarianceThreshold("Weight", 1.0d).withVarianceThreshold("Age", 5.0d)
                .withTolerance("Age", 0.2d).withTolerance("Weight", 0.06d).withVerifyRowOrder(true).verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL_3);
        this.verifier.succeeded(this.description.get());
    }

    @Test(expected = AssertionError.class)
    public void varianceFailure()
    {
        this.verifier.starting(this.description.get());
        this.verifier.withVarianceThreshold(5.0d).withVerifyRowOrder(true).verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
    }

    @Test(expected = AssertionError.class)
    public void varianceFailureForTwoColumns()
    {
        this.verifier.starting(this.description.get());
        this.verifier.withVarianceThreshold("Age", 5.0d).withVarianceThreshold("Weight", 1.0d).withVerifyRowOrder(true).verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL_3);
    }

    @Test(expected = AssertionError.class)
    public void mismatchedTypesFormatting()
    {
        this.verifier.starting(this.description.get());
        this.verifier.withVarianceThreshold(5.0d).withVerifyRowOrder(true).verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
    }

    @Test
    public void rowOrderSuccess()
    {
        this.verifier.starting(this.description.get());
        this.verifier.withTolerance(1.0).withVerifyRowOrder(false).verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
        this.verifier.succeeded(this.description.get());
    }

    @Test(expected = AssertionError.class)
    public void rowOrderFailure()
    {
        this.verifier.starting(this.description.get());
        this.verifier.verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
    }

    @Test(expected = IllegalArgumentException.class)
    public void expectedAndOutputDirsMustBeDifferent()
    {
        this.verifier.withOutputDir(TableTestUtils.getExpectedDirectory()).starting(this.description.get());
        this.verifier.verify("", null);
    }

    @Test(expected = IllegalStateException.class)
    public void ensureStartingIsCalled()
    {
        TableVerifier localVerifier = new TableVerifier().withExpectedDir(TableTestUtils.getExpectedDirectory());
        localVerifier.verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
    }

    @Test
    public void multiTableSuccess()
    {
        this.verifier.starting(this.description.get());
        this.verifier.verify(
                TableTestUtils.toNamedTables("table1", TableTestUtils.ACTUAL, "table2", TableTestUtils.ACTUAL_2),
                TableTestUtils.toNamedTables("table1", TableTestUtils.ACTUAL, "table2", TableTestUtils.ACTUAL_2));
        this.verifier.succeeded(this.description.get());
    }

    @Test(expected = AssertionError.class)
    public void multiTableFailure1()
    {
        this.verifier.starting(this.description.get());
        this.verifier.verify(
                TableTestUtils.toNamedTables("table1", TableTestUtils.ACTUAL, "table2", TableTestUtils.ACTUAL_2),
                TableTestUtils.toNamedTables("table1", TableTestUtils.ACTUAL, "table2", TableTestUtils.ACTUAL));
    }

    @Test(expected = AssertionError.class)
    public void multiTableFailure2()
    {
        this.verifier.starting(this.description.get());
        this.verifier.verify(
                TableTestUtils.toNamedTables("table1", TableTestUtils.ACTUAL, "table2", TableTestUtils.ACTUAL_2),
                TableTestUtils.toNamedTables("table1", TableTestUtils.ACTUAL_2, "table2", TableTestUtils.ACTUAL_2));
    }

    @Test(expected = AssertionError.class)
    public void multiTableMissingTable()
    {
        this.verifier.starting(this.description.get());
        this.verifier.verify(
                TableTestUtils.toNamedTables("table1", TableTestUtils.ACTUAL, "table2", TableTestUtils.ACTUAL_2),
                TableTestUtils.toNamedTables("table1", TableTestUtils.ACTUAL));
    }

    @Test(expected = AssertionError.class)
    public void multiTableSurplusTable()
    {
        this.verifier.starting(this.description.get());
        this.verifier.verify(
                TableTestUtils.toNamedTables("table1", TableTestUtils.ACTUAL),
                TableTestUtils.toNamedTables("table1", TableTestUtils.ACTUAL, "table2", TableTestUtils.ACTUAL_2));
    }

    @Test
    public void multiVerifySuccess()
    {
        this.verifier.starting(this.description.get());
        this.verifier.verify("table1", TableTestUtils.ACTUAL);
        this.verifier.verify("table2", TableTestUtils.ACTUAL_2);
        this.verifier.succeeded(this.description.get());
    }

    @Test(expected = AssertionError.class)
    public void multiVerifySurplus()
    {
        this.verifier.starting(this.description.get());
        this.verifier.verify("table1", TableTestUtils.ACTUAL);
        this.verifier.verify("table2", TableTestUtils.ACTUAL_2);
    }

    @Test(expected = AssertionError.class)
    public void multiVerifyMissing()
    {
        this.verifier.starting(this.description.get());
        this.verifier.verify("table1", TableTestUtils.ACTUAL);
        this.verifier.succeeded(this.description.get());
    }

    @Test
    public void onFailedCalledWhenMissingTable()
    {
        TestLifecycleEventHandler handler = new TestLifecycleEventHandler();
        this.verifier.withLifecycleEventHandler(handler);
        this.verifier.starting(this.description.get());
        this.verifier.verify("table1", TableTestUtils.ACTUAL);
        TableTestUtils.assertAssertionError(() -> verifier.succeeded(description.get()));
        Assert.assertEquals("started failed ", handler.lifecycle);
    }

    @Test
    public void exceptionHandlerTest()
    {
        final RuntimeException exception = new RuntimeException();
        final AtomicBoolean atomicBoolean = new AtomicBoolean(false);
        ExceptionHandler exceptionHandler = (outputFile, throwable) -> {
            Assert.assertTrue(outputFile.exists());
            Assert.assertSame(exception, throwable);
            atomicBoolean.set(true);
        };
        this.verifier.withExceptionHandler(exceptionHandler).starting(this.description.get());
        this.verifier.failed(exception, this.description.get());
        Assert.assertTrue(atomicBoolean.get());
    }

    @Test
    public void withoutPartialMatchTimeout()
    {
        this.verifier.starting(this.description.get());
        this.verifier.withoutPartialMatchTimeout().verify("table1", TableTestUtils.ACTUAL, TableTestUtils.ACTUAL);
        this.verifier.succeeded(this.description.get());
    }

    private static final Function<VerifiableTable, VerifiableTable> ACTUAL_ADAPTER = new Function<>() {
        @Override
        public VerifiableTable apply(VerifiableTable actual) {
            return new DefaultVerifiableTableAdapter(actual) {
                @Override
                public String getColumnName(int columnIndex) {
                    return "Adapted " + super.getColumnName(columnIndex);
                }
            };
        }
    };

    @Test
    public void actualAdapter()
    {
        this.verifier.starting(this.description.get());
        this.verifier.withActualAdapter(ACTUAL_ADAPTER)
          .verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
        this.verifier.succeeded(this.description.get());
    }

    @Test
    public void actualAdapterWithTableNotToAdapt()
    {
        this.verifier.starting(this.description.get());
        this.verifier.withActualAdapter(ACTUAL_ADAPTER).withTablesNotToAdapt("table1").verify(TableTestUtils.toNamedTables("table1", TableTestUtils.ACTUAL, "table2", TableTestUtils.ACTUAL_2));
        this.verifier.succeeded(this.description.get());
    }

    @Test
    public void actualAdapterNoExpectedFile()
    {
        this.verifier.starting(this.description.get());
        this.verifier.withActualAdapter(actual -> {
            Assert.assertSame(TableTestUtils.ACTUAL_2, actual);
            return TableTestUtils.ACTUAL;
        }).verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL, TableTestUtils.ACTUAL_2);
        this.verifier.succeeded(this.description.get());
    }

    private static final Function<VerifiableTable, VerifiableTable> EXPECTED_ADAPTER = new Function<>() {
        @Override
        public VerifiableTable apply(VerifiableTable actual) {
            return new DefaultVerifiableTableAdapter(actual) {
                @Override
                public String getColumnName(int columnIndex) {
                    return super.getColumnName(columnIndex).substring(8); // strip "Ignored "
                }
            };
        }
    };

    @Test
    public void expectedAdapter()
    {
        this.verifier.starting(this.description.get());
        this.verifier.withExpectedAdapter(EXPECTED_ADAPTER)
          .verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
        this.verifier.succeeded(this.description.get());
    }

    @Test
    public void expectedAdapterWithTableNotToAdapt()
    {
        this.verifier.starting(this.description.get());
        this.verifier.withExpectedAdapter(EXPECTED_ADAPTER).withTablesNotToAdapt("table1")
                .verify(TableTestUtils.toNamedTables("table1", TableTestUtils.ACTUAL, "table2", TableTestUtils.ACTUAL_2));
        this.verifier.succeeded(this.description.get());
    }

    @Test
    public void expectedAdapterNoExpectedFile()
    {
        this.verifier.starting(this.description.get());
        this.verifier.withExpectedAdapter(actual -> {
            Assert.assertSame(TableTestUtils.ACTUAL_2, actual);
            return TableTestUtils.ACTUAL;
        }).verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL_2, TableTestUtils.ACTUAL);
        this.verifier.succeeded(this.description.get());
    }

    @Test(expected = AssertionError.class)
    public void rebaseLifecycle()
    {
        TestLifecycleEventHandler handler = new TestLifecycleEventHandler();
        TableVerifier watcher = new TableVerifier().withLifecycleEventHandler(handler).withRebase();
        try
        {
            watcher.succeeded(this.description.get());
        }
        finally
        {
            Assert.assertEquals("succeeded ", handler.lifecycle);
        }
    }

    @Test
    public void rebaseAccessor()
    {
        TableVerifier tableVerifier = new TableVerifier();
        Assert.assertFalse(tableVerifier.isRebasing());
        Assert.assertTrue(tableVerifier.withRebase().isRebasing());
    }

    private static class TestLifecycleEventHandler implements LifecycleEventHandler
    {
        private String lifecycle = "";

        @Override
        public void onStarted(Description description)
        {
            this.lifecycle += "started ";
        }

        @Override
        public void onSucceeded(Description description)
        {
            this.lifecycle += "succeeded ";
        }

        @Override
        public void onFailed(Throwable e, Description description)
        {
            this.lifecycle += "failed ";
        }

        @Override
        public void onSkipped(Description description)
        {
            this.lifecycle += "skipped ";
        }

        @Override
        public void onFinished(Description description)
        {
            this.lifecycle += "finished";
        }
    }
}
