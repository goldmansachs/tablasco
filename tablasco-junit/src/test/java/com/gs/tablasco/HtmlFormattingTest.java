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

import de.skuzzle.test.snapshots.Snapshot;
import de.skuzzle.test.snapshots.junit5.EnableSnapshotTests;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@EnableSnapshotTests
public class HtmlFormattingTest {

    @RegisterExtension
    private final TableVerifier tableVerifier =
            new TableVerifier().withFilePerMethod().withMavenDirectoryStrategy();

    @Test
    void nonNumericBreak(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A9", "B1", "B9");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify("name", table1, table2));
        TableTestUtils.getHtml(this.tableVerifier);
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }

    @Test
    void numericBreak(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A", 10.123);
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A", 20.456);
        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withTolerance(0.01d).verify("name", table1, table2));
        TableTestUtils.getHtml(this.tableVerifier);
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }

    @Test
    void nonNumericActualNumericExpectedBreak(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A", 390.0);
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A", "A2");
        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withVarianceThreshold(5.0d).verify("name", table1, table2));
        TableTestUtils.getHtml(this.tableVerifier);
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }

    @Test
    void numericActualNonNumericExpectedBreak(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A", "A1");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A", 48.0);
        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withTolerance(0.1d).verify("name", table1, table2));
        TableTestUtils.getHtml(this.tableVerifier);
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }

    @Test
    void outOfOrderColumnPassedCellNonNumeric(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 2", "Col 1", "A2", "A1");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify("name", table1, table2));
        TableTestUtils.getHtml(this.tableVerifier);
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }

    @Test
    void outOfOrderColumnFailedCellNonNumeric(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 2", "Col 1", "A3", "A1");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify("name", table1, table2));
        TableTestUtils.getHtml(this.tableVerifier);
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }

    @Test
    void outOfOrderColumnPassedCellNumeric(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", 30.78, 25);
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 2", "Col 1", 25, 30.78);
        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withTolerance(0.01d).verify("name", table1, table2));
        TableTestUtils.getHtml(this.tableVerifier);
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }

    @Test
    void outOfOrderColumnFailedCellNumeric(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", 30.78, 25);
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 2", "Col 1", 25.3, 30.78);
        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withTolerance(0.01d).verify("name", table1, table2));
        TableTestUtils.getHtml(this.tableVerifier);
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }

    @Test
    void missingSurplusColumnsNonNumeric(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 4", "Col 1", "A2", "A1");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify("name", table1, table2));
        TableTestUtils.getHtml(this.tableVerifier);
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }

    @Test
    void missingSurplusColumnsNumeric(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", 30.78, 25);
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 4", "Col 1", 26, 30.78);
        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withTolerance(0.01d).verify("name", table1, table2));
        TableTestUtils.getHtml(this.tableVerifier);
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }

    @Test
    void missingSurplusRowsNonNumeric(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "C1", "C2");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify("name", table1, table2));
        TableTestUtils.getHtml(this.tableVerifier);
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }

    @Test
    void missingSurplusRowsNumeric(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", 345.66, 13.0, 56.44, 45.01);
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", 345.63, 12.8, 56.65, 45.31);
        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withTolerance(0.1d).verify("name", table1, table2));
        TableTestUtils.getHtml(this.tableVerifier);
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }

    @Test
    void assertionSummaryWithSuccess(Snapshot snapshot) throws IOException {
        VerifiableTable table = TableTestUtils.createTable(1, "Col 1", "A1");
        this.tableVerifier.withAssertionSummary(true).verify("name", table, table);
        TableTestUtils.getHtml(this.tableVerifier);
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }

    @Test
    void assertionSummaryWithFailure(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A9", "B1", "B9");
        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withAssertionSummary(true).verify("name", table1, table2));
        TableTestUtils.getHtml(this.tableVerifier);
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }

    @Test
    void assertionSummaryWithMultipleVerify(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(1, "Col 1", "A1");
        final VerifiableTable table2 = TableTestUtils.createTable(1, "Col 1", "A2");
        this.tableVerifier.withAssertionSummary(true).verify("name1", table1, table1);
        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withAssertionSummary(true).verify("name2", table1, table2));
        TableTestUtils.getHtml(this.tableVerifier);
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }

    @Test
    void assertionSummaryWithMissingSurplusTables(Snapshot snapshot) throws IOException {
        final VerifiableTable table = TableTestUtils.createTable(1, "Col 1", "A1");
        TableTestUtils.assertAssertionError(() -> tableVerifier
                .withAssertionSummary(true)
                .verify(
                        TableTestUtils.toNamedTables("name", table, "name2", table),
                        TableTestUtils.toNamedTables("name", table, "name3", table)));
        TableTestUtils.getHtml(this.tableVerifier);
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }

    @Test
    void assertionSummaryWithHideMatchedRows(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 =
                TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "C2");
        final VerifiableTable table2 =
                TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "C9");
        TableTestUtils.assertAssertionError(() -> tableVerifier
                .withAssertionSummary(true)
                .withHideMatchedRows(true)
                .verify("name", table1, table2));
        TableTestUtils.getHtml(this.tableVerifier);
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }
}
