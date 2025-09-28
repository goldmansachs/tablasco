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
public class IgnoreMissingAndSurplusTest {

    @RegisterExtension
    private final TableVerifier tableVerifier =
            new TableVerifier().withFilePerMethod().withMavenDirectoryStrategy();

    @Test
    void allRowsMatch(Snapshot snapshot) throws IOException {
        VerifiableTable table = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        this.tableVerifier.withIgnoreMissingRows().withIgnoreSurplusRows().verify("name", table, table);
        TableTestUtils.getHtml(this.tableVerifier, "table");
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier, "table"))
                .asText()
                .matchesSnapshotText();
    }

    @Test
    void failsWithSurplusAndToldToIgnoreJustMissing(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "C1", "C2", "B1", "B2");

        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withIgnoreMissingRows().verify("name", table1, table2));

        TableTestUtils.getHtml(this.tableVerifier, "table");
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier, "table"))
                .asText()
                .matchesSnapshotText();
    }

    @Test
    void failsWithMissingAndToldToIgnoreJustSurplus(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "C1", "C2", "B1", "B2");

        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withIgnoreSurplusRows().verify("name", table1, table2));
        TableTestUtils.getHtml(this.tableVerifier, "table");
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier, "table"))
                .asText()
                .matchesSnapshotText();
    }

    @Test
    void passesWithSurplusAndMissing(Snapshot snapshot) throws IOException {
        VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "C1", "C2", "B1", "B2");
        this.tableVerifier.withIgnoreMissingRows().withIgnoreSurplusRows().verify("name", table1, table2);
        TableTestUtils.getHtml(this.tableVerifier, "table");
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier, "table"))
                .asText()
                .matchesSnapshotText();
    }

    @Test
    void failsWithMissingSurplusHeader(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 3", "C1", "C2", "B1", "B2");
        TableTestUtils.assertAssertionError(() ->
                tableVerifier.withIgnoreMissingRows().withIgnoreSurplusRows().verify("name", table1, table2));
        TableTestUtils.getHtml(this.tableVerifier, "table");
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier, "table"))
                .asText()
                .matchesSnapshotText();
    }

    @Test
    void failsWithDifferenceInCommonRow(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "C1", "C2", "B1", "B3");
        TableTestUtils.assertAssertionError(() ->
                tableVerifier.withIgnoreMissingRows().withIgnoreSurplusRows().verify("name", table1, table2));
        TableTestUtils.getHtml(this.tableVerifier, "table");
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier, "table"))
                .asText()
                .matchesSnapshotText();
    }

    @Test
    void passesWithEmptyExpected(Snapshot snapshot) throws IOException {
        VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2");
        VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "C1", "C2", "B1", "B2");
        this.tableVerifier.withIgnoreMissingRows().withIgnoreSurplusRows().verify("name", table1, table2);
        TableTestUtils.getHtml(this.tableVerifier, "table");
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier, "table"))
                .asText()
                .matchesSnapshotText();
    }

    @Test
    void passesWithEmptyActual(Snapshot snapshot) throws IOException {
        VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "C1", "C2", "B1", "B2");
        VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2");
        this.tableVerifier.withIgnoreMissingRows().withIgnoreSurplusRows().verify("name", table1, table2);
        TableTestUtils.getHtml(this.tableVerifier, "table");
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier, "table"))
                .asText()
                .matchesSnapshotText();
    }

    @Test
    void ignoreSurplusColumnsPassesWithSurplus(Snapshot snapshot) throws IOException {
        VerifiableTable expected = TableTestUtils.createTable(2, "Col 1", "Col 3", "A1", "A3");
        VerifiableTable actual = TableTestUtils.createTable(3, "Col 1", "Col 2", "Col 3", "A1", "A2", "A3");
        this.tableVerifier.withIgnoreSurplusColumns().verify("name", expected, actual);
        TableTestUtils.getHtml(this.tableVerifier, "table");
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier, "table"))
                .asText()
                .matchesSnapshotText();
    }

    @Test
    void ignoreSurplusColumnsFailsWithMissing(Snapshot snapshot) throws IOException {
        final VerifiableTable expected = TableTestUtils.createTable(2, "Col 1", "Col 3", "A1", "A3");
        final VerifiableTable actual = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2");
        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withIgnoreSurplusColumns().verify("name", expected, actual));
        TableTestUtils.getHtml(this.tableVerifier, "table");
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier, "table"))
                .asText()
                .matchesSnapshotText();
    }

    @Test
    void ignoreMissingColumnsPassesWithMissing(Snapshot snapshot) throws IOException {
        VerifiableTable expected = TableTestUtils.createTable(3, "Col 1", "Col 2", "Col 3", "A1", "A2", "A3");
        VerifiableTable actual = TableTestUtils.createTable(2, "Col 1", "Col 3", "A1", "A3");
        this.tableVerifier.withIgnoreMissingColumns().verify("name", expected, actual);
        TableTestUtils.getHtml(this.tableVerifier, "table");
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier, "table"))
                .asText()
                .matchesSnapshotText();
    }

    @Test
    void ignoreMissingColumnsFailsWithSurplus(Snapshot snapshot) throws IOException {
        final VerifiableTable expected = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2");
        final VerifiableTable actual = TableTestUtils.createTable(2, "Col 1", "Col 3", "A1", "A3");
        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withIgnoreMissingColumns().verify("name", expected, actual));
        TableTestUtils.getHtml(this.tableVerifier, "table");
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier, "table"))
                .asText()
                .matchesSnapshotText();
    }

    @Test
    void ignoreMissingAndSurplusColumnsPasses(Snapshot snapshot) throws IOException {
        VerifiableTable expected = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2");
        VerifiableTable actual = TableTestUtils.createTable(2, "Col 1", "Col 3", "A1", "A3");
        this.tableVerifier.withIgnoreMissingColumns().withIgnoreSurplusColumns().verify("name", expected, actual);
        TableTestUtils.getHtml(this.tableVerifier, "table");
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier, "table"))
                .asText()
                .matchesSnapshotText();
    }
}
