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

import com.gs.tablasco.files.MavenStyleDirectoryStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class MavenStyleDirectoryStrategyTest {

    @RegisterExtension
    private final TableVerifier tableVerifier = new TableVerifier()
            .withFilePerClass()
            .withDirectoryStrategy(new MavenStyleDirectoryStrategy()
                    .withAnchorFile("pom.xml")
                    .withExpectedSubDir("maven_input")
                    .withOutputSubDir("maven_output"));

    @Test
    void testMavenStyleDirectoryStrategy() {
        this.tableVerifier.verify(
                "maven", new TestTable("h1", "h2").withRow("r11", "r12").withRow("r21", "r22"));
    }
}
