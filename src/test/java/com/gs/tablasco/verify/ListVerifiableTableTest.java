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

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ListVerifiableTableTest
{
    @Test
    public void testHeaderTypes() throws Exception
    {
        // old usages prior to allowing String headers
        List<Object> headersAsObjects = Collections.<Object>singletonList("Col");
        List<List<Object>> headersAndDataAsObjects = Arrays.asList(headersAsObjects, Collections.<Object>singletonList("Val"));
        Assert.assertEquals("Test constructor with headers and rows in one List<List<Object>>", 1, new ListVerifiableTable(headersAndDataAsObjects).getRowCount());
        Assert.assertEquals("Test constructor with headers as List<Object>", 2, new ListVerifiableTable(headersAsObjects, headersAndDataAsObjects).getRowCount());
        List<String> headersAsStrings = Collections.singletonList("Col");
        Assert.assertEquals("Test cast that used to be necessary for headers as List<String>", 2, new ListVerifiableTable((List) headersAsStrings, headersAndDataAsObjects).getRowCount());

        // allow passing string headers
        Assert.assertEquals("Test headers as List<String> can now be passed in as-is", 2, new ListVerifiableTable(headersAsStrings, headersAndDataAsObjects).getRowCount());
    }
}
