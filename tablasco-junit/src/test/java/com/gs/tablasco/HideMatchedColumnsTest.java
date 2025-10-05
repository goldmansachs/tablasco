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

import com.gs.tablasco.verify.KeyedVerifiableTableAdapter;
import de.skuzzle.test.snapshots.Snapshot;
import de.skuzzle.test.snapshots.junit5.EnableSnapshotTests;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@EnableSnapshotTests
public class HideMatchedColumnsTest {

    @RegisterExtension
    private final TableVerifier tableVerifier =
            new TableVerifier().withFilePerMethod().withMavenDirectoryStrategy().withHideMatchedColumns(true);

    @Test
    void allColumnsMatch(Snapshot snapshot) throws IOException {
        VerifiableTable table = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        this.tableVerifier.verify("name", table, table);
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }

    @Test
    void missingAndSurplusRows(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(
                3, "Col 1", "Col 2", "Col 3", "A1", "A2", "A3", "B1", "B2", "B3", "C1", "C2", "C3");
        final VerifiableTable table2 = TableTestUtils.createTable(
                3, "Col 1", "Col 2", "Col 3", "B1", "B2", "B9", "C1", "C2", "C9", "D1", "D2", "D3");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify("name", table1, table2));
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }

    @Test
    void multiMatchedColumns(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(
                8, "Col 1", "Col 2", "Col 3", "Col 4", "Col 5", "Col 6", "Col 7", "Col 8", "A", "A", "A", "A", "A", "A",
                "A", "A", "B", "B", "B", "B", "B", "B", "B", "B");
        final VerifiableTable table2 = TableTestUtils.createTable(
                8, "Col 1", "Col 2", "Col 3", "Col 4", "Col 5", "Col 6", "Col 7", "Col 8", "A", "A", "A", "A", "A", "X",
                "A", "A", "B", "B", "X", "B", "B", "B", "B", "B");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify("name", table1, table2));
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }

    @Test
    void keyColumnIgnored(Snapshot snapshot) throws IOException {
        VerifiableTable table = new KeyedVerifiableTableAdapter(
                TableTestUtils.createTable(3, "Col 1", "Col 2", "Col 3", "A", "A", "A"), 0);
        this.tableVerifier.verify("name", table, table);
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }

    @Test
    void matchedRowsAndColumns(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(
                4, "Col 1", "Col 2", "Col 3", "Col 3", "A", "A", "A", "A", "B", "B", "B", "B", "C", "C", "C", "C", "D",
                "D", "D", "D");
        final VerifiableTable table2 = TableTestUtils.createTable(
                4, "Col 1", "Col 2", "Col 3", "Col 3", "A", "A", "A", "X", "B", "B", "B", "B", "C", "C", "C", "C", "X",
                "D", "D", "D");
        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withHideMatchedRows(true).verify("name", table1, table2));
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }
}
