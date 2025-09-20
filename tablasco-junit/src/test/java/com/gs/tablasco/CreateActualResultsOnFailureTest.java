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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(TablascoExtension.class)
public class CreateActualResultsOnFailureTest {
    private final TableVerifier verifier =
            new TableVerifier().withMavenDirectoryStrategy().withFilePerMethod();

    @RegisterExtension
    public final TableTestUtils.TestDescription description = new TableTestUtils.TestDescription();

    @BeforeEach
    void setUp() {
        this.verifier.starting(this.description.get());
        this.verifier.getActualFile().delete();
    }

    @Test
    void testTrue() {
        this.verifier.withCreateActualResultsOnFailure(true).verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
        assertFalse(this.verifier.getActualFile().exists());
    }

    @Test
    void testFalse() {
        this.verifier.withCreateActualResultsOnFailure(false).verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
        assertTrue(this.verifier.getActualFile().exists());
    }

    @Test
    void testTrueFail() {
        TableTestUtils.assertAssertionError(() -> verifier.withCreateActualResultsOnFailure(true)
                .verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL));
        assertTrue(this.verifier.getActualFile().exists());
    }

    @Test
    void testFalseFail() {
        TableTestUtils.assertAssertionError(() -> verifier.withCreateActualResultsOnFailure(false)
                .verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL));
        assertTrue(this.verifier.getActualFile().exists());
    }
}
