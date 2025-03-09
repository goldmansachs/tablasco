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

import static org.junit.Assert.assertEquals;

import org.junit.Assert;
import org.junit.Test;

public class MetadataTest {
    @Test
    public void testMetadataCreatedForRecordingResults() throws Exception {
        Metadata metadata = Metadata.newWithRecordedAt();
        metadata.add("App Server URL", "http://test");
        metadata.add("testKey", "testValue");
        String asString = metadata.toString("#");
        Assert.assertTrue(asString.contains("#Recorded At#"));
        Assert.assertTrue(asString.contains("#App Server URL# #http://test#, #testKey# #testValue#"));
    }

    @Test
    public void testMetadataCreatedForParsingResults() throws Exception {
        Metadata metadata = Metadata.newEmpty();
        metadata.add("testKey", "testValue");
        metadata.add("App Server URL", "http://test");

        String asString = metadata.toString();
        assertEquals("testKey testValue, App Server URL http://test", asString);
    }
}
