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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

//import com.gs.fw.common.base.exception.CollectedException;

public class ExceptionHtmlTest
{
    @Test
    public void testException() throws IOException
    {
        String stackTraceToString = ExceptionHtml.stackTraceToString(new RuntimeException(new IllegalArgumentException(new UnsupportedOperationException())));
        List<Map.Entry<String, List<String>>> stackTraces = getStackLineCount(stackTraceToString);
        Assert.assertEquals(3, stackTraces.size());
        Assert.assertEquals(RuntimeException.class.getName() + ": " + IllegalArgumentException.class.getName() + ": " + UnsupportedOperationException.class.getName(), stackTraces.get(0).getKey());
        Assert.assertTrue(stackTraces.get(0).getValue().size() > 1);
        Assert.assertEquals("Caused by: " + IllegalArgumentException.class.getName() + ": " + UnsupportedOperationException.class.getName(), stackTraces.get(1).getKey());
        Assert.assertTrue(stackTraces.get(1).getValue().size() > 1);
        Assert.assertEquals("Caused by: " + UnsupportedOperationException.class.getName(), stackTraces.get(2).getKey());
        Assert.assertTrue(stackTraces.get(2).getValue().size() > 1);
    }

    /*
    @Test
    public void testCollectedException() throws IOException
    {
        String stackTraceToString = ""; //ExceptionHtml.stackTraceToString(buildIntricateCollectedException());
        List<Pair<String, List<String>>> stackTraces = getStackLineCount(stackTraceToString);
        Assert.assertEquals(9, stackTraces.size());
        int index = 0;
        Assert.assertEquals("com.gs.fw.common.base.exception.CollectedException: foo", stackTraces.get(index++).getOne());
        Assert.assertEquals("***************** Begin Collected Exception 1 of 2 *****************", stackTraces.get(index++).getOne());
        Assert.assertEquals("java.lang.IllegalArgumentException: m1", stackTraces.get(index++).getOne());
        Assert.assertEquals("Caused by: java.lang.UnsupportedOperationException: m2", stackTraces.get(index++).getOne());
        Assert.assertEquals("***************** End Collected Exception 1 of 2 *****************", stackTraces.get(index++).getOne());
        Assert.assertEquals("***************** Begin Collected Exception 2 of 2 *****************", stackTraces.get(index++).getOne());
        Assert.assertEquals("java.lang.UnsupportedOperationException: m3", stackTraces.get(index++).getOne());
        Assert.assertEquals("Caused by: java.lang.IllegalArgumentException: m4", stackTraces.get(index++).getOne());
        Assert.assertEquals("***************** End Collected Exception 2 of 2 *****************", stackTraces.get(index).getOne());
    }

    @Test
    public void testCollectedExceptionAsCause() throws IOException
    {
        String stackTraceToString = "";// ExceptionHtml.stackTraceToString(new IllegalArgumentException(buildIntricateCollectedException()));
        List<Pair<String, List<String>>> stackTraces = getStackLineCount(stackTraceToString);
        Assert.assertEquals(10, stackTraces.size());
        int index = 0;
        Assert.assertEquals("java.lang.IllegalArgumentException: com.gs.fw.common.base.exception.CollectedException: foo", stackTraces.get(index++).getOne());
        Assert.assertEquals("Caused by: com.gs.fw.common.base.exception.CollectedException: foo", stackTraces.get(index++).getOne());
        Assert.assertEquals("***************** Begin Collected Exception 1 of 2 *****************", stackTraces.get(index++).getOne());
        Assert.assertEquals("java.lang.IllegalArgumentException: m1", stackTraces.get(index++).getOne());
        Assert.assertEquals("Caused by: java.lang.UnsupportedOperationException: m2", stackTraces.get(index++).getOne());
        Assert.assertEquals("***************** End Collected Exception 1 of 2 *****************", stackTraces.get(index++).getOne());
        Assert.assertEquals("***************** Begin Collected Exception 2 of 2 *****************", stackTraces.get(index++).getOne());
        Assert.assertEquals("java.lang.UnsupportedOperationException: m3", stackTraces.get(index++).getOne());
        Assert.assertEquals("Caused by: java.lang.IllegalArgumentException: m4", stackTraces.get(index++).getOne());
        Assert.assertEquals("***************** End Collected Exception 2 of 2 *****************", stackTraces.get(index).getOne());
    }

    private static CollectedException buildIntricateCollectedException()
    {
        return new CollectedException("foo", FastList.<Throwable>newListWith(
                new IllegalArgumentException("m1", new UnsupportedOperationException("m2")),
                new UnsupportedOperationException("m3", new IllegalArgumentException("m4"))));
    }
    */

    private static List<Map.Entry<String, List<String>>> getStackLineCount(String string) throws IOException
    {
        Map<String, List<String>> stackTraces = new LinkedHashMap<>();
        List<String> stackTrace = null;
        BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(string.getBytes())));
        String line = reader.readLine();
        while (line != null)
        {
            if (line.startsWith("    "))
            {
                stackTrace.add(line);
            }
            else
            {
                stackTrace = new ArrayList<>();
                stackTraces.put(line, stackTrace);
            }
            line = reader.readLine();
        }
        return new ArrayList<>(stackTraces.entrySet());
    }
}
