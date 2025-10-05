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
public class HideMatchedTablesTest {

    @RegisterExtension
    private final TableVerifier tableVerifier =
            new TableVerifier().withFilePerMethod().withMavenDirectoryStrategy().withHideMatchedTables(true);

    @Test
    void matchedTablesAreHidden(Snapshot snapshot) throws IOException {
        final VerifiableTable matchTable = new TestTable("Col").withRow("A");
        final VerifiableTable outOfOrderTableExpected = new TestTable("Col 1", "Col 2").withRow("A", "B");
        final VerifiableTable outOfOrderTableActual = new TestTable("Col 2", "Col 1").withRow("B", "A");
        final VerifiableTable breakTableExpected = new TestTable("Col").withRow("A");
        final VerifiableTable breakTableActual = new TestTable("Col").withRow("B");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify(
                TableTestUtils.toNamedTables(
                        "match", matchTable, "break", breakTableExpected, "outOfOrder", outOfOrderTableExpected),
                TableTestUtils.toNamedTables(
                        "match", matchTable, "break", breakTableActual, "outOfOrder", outOfOrderTableActual)));
        TableTestUtils.getHtml(this.tableVerifier);
        snapshot.assertThat(TableTestUtils.getHtml(this.tableVerifier)).asText().matchesSnapshotText();
    }
}
