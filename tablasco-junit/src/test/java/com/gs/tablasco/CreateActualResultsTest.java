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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class CreateActualResultsTest {
    private final TableVerifier verifier =
            new TableVerifier().withMavenDirectoryStrategy().withFilePerMethod();

    @Rule
    public final TableTestUtils.TestDescription description = new TableTestUtils.TestDescription();

    @Before
    public void setUp() {
        this.verifier.starting(this.description.get());
        this.verifier.getActualFile().delete();
    }

    @Test
    public void testDefault() {
        this.verifier.verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
        this.verifier.succeeded(this.description.get());
        Assert.assertTrue(this.verifier.getActualFile().exists());
    }

    @Test
    public void testTrue() {
        this.verifier.withCreateActualResults(true).verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
        this.verifier.succeeded(this.description.get());
        Assert.assertTrue(this.verifier.getActualFile().exists());
    }

    @Test
    public void testFalse() {
        this.verifier.withCreateActualResults(false).verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
        this.verifier.succeeded(this.description.get());
        Assert.assertFalse(this.verifier.getActualFile().exists());
    }
}
