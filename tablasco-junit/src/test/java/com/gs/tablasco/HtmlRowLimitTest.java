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
public class HtmlRowLimitTest {

    @RegisterExtension
    private final TableVerifier tableVerifier =
            new TableVerifier().withFilePerMethod().withMavenDirectoryStrategy().withHtmlRowLimit(3);

    @Test
    void tablesMatch(Snapshot snapshot) throws IOException {
        VerifiableTable table = TableTestUtils.createTable(
                2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "C2", "D1", "D2", "E1", "E2");
        this.tableVerifier.verify("name", table, table);
        TableTestUtils.getHtml(this.tableVerifier, "table");
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier, "table"))
                .asText()
                .matchesSnapshotText();
    }

    @Test
    void tablesDoNotMatch(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(
                2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "C2", "D1", "D2", "E1", "E2");
        final VerifiableTable table2 = TableTestUtils.createTable(
                2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "C2", "D1", "DX", "E1", "E2");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify("name", table1, table2));
        TableTestUtils.getHtml(this.tableVerifier, "table");
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier, "table"))
                .asText()
                .matchesSnapshotText();
    }

    @Test
    void hideMatchedRows(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(
                2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "C2", "D1", "D2", "E1", "E2");
        final VerifiableTable table2 = TableTestUtils.createTable(
                2, "Col 1", "Col 2", "A1", "AX", "B1", "B2", "C1", "C2", "D1", "DX", "E1", "E2");
        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withHideMatchedRows(true).verify("name", table1, table2));
        TableTestUtils.getHtml(this.tableVerifier, "table");
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier, "table"))
                .asText()
                .matchesSnapshotText();
    }

    @Test
    void hideMatchedRows2(Snapshot snapshot) throws IOException {
        final VerifiableTable table1 =
                TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "C2", "D1", "D2");
        final VerifiableTable table2 =
                TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "CX", "D1", "DX");
        TableTestUtils.assertAssertionError(() ->
                tableVerifier.withHtmlRowLimit(1).withHideMatchedRows(true).verify("name", table1, table2));
        TableTestUtils.getHtml(this.tableVerifier, "table");
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier, "table"))
                .asText()
                .matchesSnapshotText();
    }
}
