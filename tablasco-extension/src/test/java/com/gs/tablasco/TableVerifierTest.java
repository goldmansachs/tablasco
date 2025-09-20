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

import static org.junit.jupiter.api.Assertions.*;

import com.gs.tablasco.verify.DefaultVerifiableTableAdapter;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class TableVerifierTest {
    private final TableVerifier verifier = new TableVerifier()
            .withExpectedDir(TableTestUtils.getExpectedDirectory())
            .withOutputDir(TableTestUtils.getOutputDirectory())
            .withFilePerClass();

    @RegisterExtension
    public final TableTestUtils.TestDescription description = new TableTestUtils.TestDescription();

    @Test
    void validationSuccess() {
        this.verifier.starting(this.description.get());
        this.verifier.verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
        this.verifier.succeeded(this.description.get());
    }

    @Test
    void validationFailure() {
        assertThrows(AssertionError.class, () -> {
            this.verifier.starting(this.description.get());
            this.verifier.verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
        });
    }

    @Test
    void toleranceSuccess() {
        this.verifier.starting(this.description.get());
        this.verifier
                .withTolerance(0.2d)
                .withVerifyRowOrder(true)
                .verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
        this.verifier.succeeded(this.description.get());
    }

    @Test
    void toleranceSuccessForFirstColumn() {
        this.verifier.starting(this.description.get());
        this.verifier.withTolerance("Age", 0.2d).verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL_3);
        this.verifier.succeeded(this.description.get());
    }

    @Test
    void toleranceSuccessForSecondColumn() {
        this.verifier.starting(this.description.get());
        this.verifier.withTolerance("Weight", 0.06d).verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL_3);
        this.verifier.succeeded(this.description.get());
    }

    @Test
    void toleranceSuccessForTwoColumns() {
        this.verifier.starting(this.description.get());
        this.verifier
                .withTolerance("Weight", 0.06d)
                .withTolerance("Age", 0.2d)
                .verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL_3);
        this.verifier.succeeded(this.description.get());
    }

    @Test
    void toleranceSuccessWithGeneralCase() {
        this.verifier.starting(this.description.get());
        this.verifier
                .withTolerance("Weight", 0.06d)
                .withTolerance(1.0d)
                .verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL_3);
        this.verifier.succeeded(this.description.get());
    }

    @Test
    void toleranceFailure() {
        assertThrows(AssertionError.class, () -> {
            this.verifier.starting(this.description.get());
            this.verifier
                    .withTolerance(0.1d)
                    .withVerifyRowOrder(true)
                    .verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
        });
    }

    @Test
    void toleranceFailureForTwoColumns() {
        assertThrows(AssertionError.class, () -> {
            this.verifier.starting(this.description.get());
            this.verifier
                    .withTolerance("Age", 0.2d)
                    .withTolerance("Weight", 0.06d)
                    .withVerifyRowOrder(true)
                    .verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL_3);
        });
    }

    @Test
    void toleranceFailureWithGeneralCase() {
        assertThrows(AssertionError.class, () -> {
            this.verifier.starting(this.description.get());
            this.verifier
                    .withTolerance("Weight", 0.06d)
                    .withTolerance(1.0d)
                    .verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL_3);
        });
    }

    @Test
    void varianceSuccess() {
        this.verifier.starting(this.description.get());
        this.verifier
                .withVarianceThreshold(5.0d)
                .withVerifyRowOrder(true)
                .verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
        this.verifier.succeeded(this.description.get());
    }

    @Test
    void varianceSuccessForTwoColumns() {
        this.verifier.starting(this.description.get());
        this.verifier
                .withVarianceThreshold("Weight", 1.0d)
                .withVarianceThreshold("Age", 5.0d)
                .withVerifyRowOrder(true)
                .verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL_3);
        this.verifier.succeeded(this.description.get());
    }

    @Test
    void varianceSuccessWithTolerance() {
        this.verifier.starting(this.description.get());
        this.verifier
                .withVarianceThreshold("Weight", 1.0d)
                .withVarianceThreshold("Age", 5.0d)
                .withTolerance("Age", 0.2d)
                .withTolerance("Weight", 0.06d)
                .withVerifyRowOrder(true)
                .verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL_3);
        this.verifier.succeeded(this.description.get());
    }

    @Test
    void varianceFailure() {
        assertThrows(AssertionError.class, () -> {
            this.verifier.starting(this.description.get());
            this.verifier
                    .withVarianceThreshold(5.0d)
                    .withVerifyRowOrder(true)
                    .verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
        });
    }

    @Test
    void varianceFailureForTwoColumns() {
        assertThrows(AssertionError.class, () -> {
            this.verifier.starting(this.description.get());
            this.verifier
                    .withVarianceThreshold("Age", 5.0d)
                    .withVarianceThreshold("Weight", 1.0d)
                    .withVerifyRowOrder(true)
                    .verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL_3);
        });
    }

    @Test
    void mismatchedTypesFormatting() {
        assertThrows(AssertionError.class, () -> {
            this.verifier.starting(this.description.get());
            this.verifier
                    .withVarianceThreshold(5.0d)
                    .withVerifyRowOrder(true)
                    .verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
        });
    }

    @Test
    void rowOrderSuccess() {
        this.verifier.starting(this.description.get());
        this.verifier
                .withTolerance(1.0)
                .withVerifyRowOrder(false)
                .verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
        this.verifier.succeeded(this.description.get());
    }

    @Test
    void rowOrderFailure() {
        assertThrows(AssertionError.class, () -> {
            this.verifier.starting(this.description.get());
            this.verifier.verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
        });
    }

    @Test
    void expectedAndOutputDirsMustBeDifferent() {
        assertThrows(IllegalArgumentException.class, () -> {
            this.verifier.withOutputDir(TableTestUtils.getExpectedDirectory()).starting(this.description.get());
            this.verifier.verify("", null);
        });
    }

    @Test
    void ensureStartingIsCalled() {
        assertThrows(IllegalStateException.class, () -> {
            TableVerifier localVerifier = new TableVerifier().withExpectedDir(TableTestUtils.getExpectedDirectory());
            localVerifier.verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
        });
    }

    @Test
    void multiTableSuccess() {
        this.verifier.starting(this.description.get());
        this.verifier.verify(
                TableTestUtils.toNamedTables("table1", TableTestUtils.ACTUAL, "table2", TableTestUtils.ACTUAL_2),
                TableTestUtils.toNamedTables("table1", TableTestUtils.ACTUAL, "table2", TableTestUtils.ACTUAL_2));
        this.verifier.succeeded(this.description.get());
    }

    @Test
    void multiTableFailure1() {
        assertThrows(AssertionError.class, () -> {
            this.verifier.starting(this.description.get());
            this.verifier.verify(
                    TableTestUtils.toNamedTables("table1", TableTestUtils.ACTUAL, "table2", TableTestUtils.ACTUAL_2),
                    TableTestUtils.toNamedTables("table1", TableTestUtils.ACTUAL, "table2", TableTestUtils.ACTUAL));
        });
    }

    @Test
    void multiTableFailure2() {
        assertThrows(AssertionError.class, () -> {
            this.verifier.starting(this.description.get());
            this.verifier.verify(
                    TableTestUtils.toNamedTables("table1", TableTestUtils.ACTUAL, "table2", TableTestUtils.ACTUAL_2),
                    TableTestUtils.toNamedTables("table1", TableTestUtils.ACTUAL_2, "table2", TableTestUtils.ACTUAL_2));
        });
    }

    @Test
    void multiTableMissingTable() {
        assertThrows(AssertionError.class, () -> {
            this.verifier.starting(this.description.get());
            this.verifier.verify(
                    TableTestUtils.toNamedTables("table1", TableTestUtils.ACTUAL, "table2", TableTestUtils.ACTUAL_2),
                    TableTestUtils.toNamedTables("table1", TableTestUtils.ACTUAL));
        });
    }

    @Test
    void multiTableSurplusTable() {
        assertThrows(AssertionError.class, () -> {
            this.verifier.starting(this.description.get());
            this.verifier.verify(
                    TableTestUtils.toNamedTables("table1", TableTestUtils.ACTUAL),
                    TableTestUtils.toNamedTables("table1", TableTestUtils.ACTUAL, "table2", TableTestUtils.ACTUAL_2));
        });
    }

    @Test
    void multiVerifySuccess() {
        this.verifier.starting(this.description.get());
        this.verifier.verify("table1", TableTestUtils.ACTUAL);
        this.verifier.verify("table2", TableTestUtils.ACTUAL_2);
        this.verifier.succeeded(this.description.get());
    }

    @Test
    void multiVerifySurplus() {
        assertThrows(AssertionError.class, () -> {
            this.verifier.starting(this.description.get());
            this.verifier.verify("table1", TableTestUtils.ACTUAL);
            this.verifier.verify("table2", TableTestUtils.ACTUAL_2);
        });
    }

    @Test
    void multiVerifyMissing() {
        assertThrows(AssertionError.class, () -> {
            this.verifier.starting(this.description.get());
            this.verifier.verify("table1", TableTestUtils.ACTUAL);
            this.verifier.succeeded(this.description.get());
        });
    }

    @Test
    void withoutPartialMatchTimeout() {
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
    void actualAdapter() {
        this.verifier.starting(this.description.get());
        this.verifier.withActualAdapter(ACTUAL_ADAPTER).verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
        this.verifier.succeeded(this.description.get());
    }

    @Test
    void actualAdapterWithTableNotToAdapt() {
        this.verifier.starting(this.description.get());
        this.verifier
                .withActualAdapter(ACTUAL_ADAPTER)
                .withTablesNotToAdapt("table1")
                .verify(TableTestUtils.toNamedTables(
                        "table1", TableTestUtils.ACTUAL, "table2", TableTestUtils.ACTUAL_2));
        this.verifier.succeeded(this.description.get());
    }

    @Test
    void actualAdapterNoExpectedFile() {
        this.verifier.starting(this.description.get());
        this.verifier
                .withActualAdapter(actual -> {
                    assertSame(TableTestUtils.ACTUAL_2, actual);
                    return TableTestUtils.ACTUAL;
                })
                .verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL, TableTestUtils.ACTUAL_2);
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
    void expectedAdapter() {
        this.verifier.starting(this.description.get());
        this.verifier.withExpectedAdapter(EXPECTED_ADAPTER).verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
        this.verifier.succeeded(this.description.get());
    }

    @Test
    void expectedAdapterWithTableNotToAdapt() {
        this.verifier.starting(this.description.get());
        this.verifier
                .withExpectedAdapter(EXPECTED_ADAPTER)
                .withTablesNotToAdapt("table1")
                .verify(TableTestUtils.toNamedTables(
                        "table1", TableTestUtils.ACTUAL, "table2", TableTestUtils.ACTUAL_2));
        this.verifier.succeeded(this.description.get());
    }

    @Test
    void expectedAdapterNoExpectedFile() {
        this.verifier.starting(this.description.get());
        this.verifier
                .withExpectedAdapter(actual -> {
                    assertSame(TableTestUtils.ACTUAL_2, actual);
                    return TableTestUtils.ACTUAL;
                })
                .verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL_2, TableTestUtils.ACTUAL);
        this.verifier.succeeded(this.description.get());
    }

    @Test
    void rebaseAccessor() {
        TableVerifier tableVerifier = new TableVerifier();
        assertFalse(tableVerifier.isRebasing());
        assertTrue(tableVerifier.withRebase().isRebasing());
    }
}
