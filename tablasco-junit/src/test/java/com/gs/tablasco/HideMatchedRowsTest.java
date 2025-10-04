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
public class HideMatchedRowsTest {

    @RegisterExtension
    private final TableVerifier tableVerifier =
            new TableVerifier().withFilePerMethod().withMavenDirectoryStrategy().withHideMatchedRows(true);

    @Test
    void allRowsMatch(Snapshot snapshot) throws IOException {
        VerifiableTable table = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        this.tableVerifier.verify("name", table, table);
        TableTestUtils.getHtml(this.tableVerifier);
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }

    @Test
    void alwaysShowMatchedRowsFor(Snapshot snapshot) throws IOException {
        final VerifiableTable table = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        TableTestUtils.assertAssertionError(() -> tableVerifier
                .withAlwaysShowMatchedRowsFor("name2")
                .verify(
                        TableTestUtils.toNamedTables("name", table, "name2", table, "name3", table),
                        TableTestUtils.toNamedTables("name", table, "name2", table, "name4", table)));
        TableTestUtils.getHtml(this.tableVerifier);
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }

    @Test
    void outOfOrderRowMatch(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "B1", "B2", "A1", "A2");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify("name", table1, table2));
        TableTestUtils.getHtml(this.tableVerifier);
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }

    @Test
    void allRowsFail(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A9", "B1", "B9");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify("name", table1, table2));
        TableTestUtils.getHtml(this.tableVerifier);
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }

    @Test
    void missingSurplusColumns(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(
                3, "Col 1", "Col 2", "Col 3", "A1", "A2", "A3", "B1", "B2", "B3", "C1", "C2", "C3");
        final VerifiableTable table2 = TableTestUtils.createTable(
                3, "Col 3", "Col 4", "Col 1", "A3", "A2", "A1", "B3", "B2", "B9", "C3", "C2", "C1");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify("name", table1, table2));
        TableTestUtils.getHtml(this.tableVerifier);
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }

    @Test
    void someRowsMatch(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(
                2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "C2", "D1", "D2", "E1", "E2", "F1", "F2", "G1", "G2",
                "H1", "H2");
        final VerifiableTable table2 = TableTestUtils.createTable(
                2, "Col 1", "Col 2", "A1", "A2", "B1", "X2", "C1", "C2", "D1", "D2", "E1", "X2", "F1", "F2", "G1", "G2",
                "H1", "H2");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify("name", table1, table2));
        TableTestUtils.getHtml(this.tableVerifier);
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }
}
