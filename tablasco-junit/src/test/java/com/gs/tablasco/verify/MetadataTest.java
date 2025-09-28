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

package com.gs.tablasco.verify;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class MetadataTest {
    @Test
    void testMetadataCreatedForRecordingResults() {
        Metadata metadata = Metadata.newWithRecordedAt();
        metadata.add("App Server URL", "http://test");
        metadata.add("testKey", "testValue");
        String asString = metadata.toString("#");
        assertTrue(asString.contains("#Recorded At#"));
        assertTrue(asString.contains("#App Server URL# #http://test#, #testKey# #testValue#"));
    }

    @Test
    void testMetadataCreatedForParsingResults() {
        Metadata metadata = Metadata.newEmpty();
        metadata.add("testKey", "testValue");
        metadata.add("App Server URL", "http://test");

        String asString = metadata.toString();
        assertEquals("testKey testValue, App Server URL http://test", asString);
    }
}
