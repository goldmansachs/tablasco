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

import com.gs.tablasco.lifecycle.LifecycleEventHandler;
import org.eclipse.collections.impl.factory.Maps;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LifecycleTest
{
    private static final Map<Class, List<String>> EVENTS = UnifiedMap.newMap();

    // todo: rebase, missing table, rebase exception

    @Test
    public void passedTest() throws Exception
    {
        Class testClass = LifecycleTestPassedInnerTest.class;
        Result result = runInnerTest(testClass);
        Assert.assertTrue(result.wasSuccessful());
        Assert.assertTrue(getOutputFile(testClass).exists());
        Assert.assertEquals(Arrays.asList("onStarted", "onSucceeded", "onFinished"), EVENTS.get(testClass));
    }

    @Test
    public void failedTest() throws Exception
    {
        Class testClass = LifecycleTestFailedInnerTest.class;
        Result result = runInnerTest(testClass);
        Assert.assertFalse(result.wasSuccessful());
        Assert.assertTrue(getOutputFile(testClass).exists());
        Assert.assertEquals(Arrays.asList("onStarted", "onFailed", "onFinished"), EVENTS.get(testClass));
    }

    @Test
    public void exceptionTest() throws Exception
    {
        Class testClass = LifecycleTestExceptionInnerTest.class;
        Result result = runInnerTest(testClass);
        Assert.assertFalse(result.wasSuccessful());
        Assert.assertTrue(getOutputFile(testClass).exists());
        Assert.assertEquals(Arrays.asList("onStarted", "onFailed", "onFinished"), EVENTS.get(testClass));
    }

    @Test
    public void rebaseTest() throws Exception
    {
        Class testClass = LifecycleTestRebaseInnerTest.class;
        Result result = runInnerTest(testClass);
        Assert.assertFalse(result.wasSuccessful());
        Assert.assertFalse(getOutputFile(testClass).exists());
        Assert.assertTrue(new File(new File(TableTestUtils.getOutputDirectory(), "expected"), testClass.getSimpleName() + ".txt").exists());
        Assert.assertEquals(Arrays.asList("onStarted", "onSucceeded", "onFinished"), EVENTS.get(testClass));
    }

    @Test
    public void rebaseExceptionTest() throws Exception
    {
        Class testClass = LifecycleTestRebaseExceptionInnerTest.class;
        Result result = runInnerTest(testClass);
        Assert.assertFalse(result.wasSuccessful());
        Assert.assertTrue(getOutputFile(testClass).exists());
        Assert.assertFalse(new File(new File(TableTestUtils.getOutputDirectory(), "expected"), testClass.getSimpleName() + ".txt").exists());
        Assert.assertEquals(Arrays.asList("onStarted", "onFailed", "onFinished"), EVENTS.get(testClass));
    }

    @Test
    public void  missingTableTest() throws Exception
    {
        Class testClass = LifecycleTestMissingTableInnerTest.class;
        Result result = runInnerTest(testClass);
        Assert.assertFalse(result.wasSuccessful());
        Assert.assertTrue(getOutputFile(testClass).exists());
        Assert.assertEquals(Arrays.asList("onStarted", "onFailed", "onFinished"), EVENTS.get(testClass));
    }

    private static File getOutputFile(Class innerClass)
    {
        return new File(TableTestUtils.getOutputDirectory(), innerClass.getSimpleName() + ".html");
    }

    private static Result runInnerTest(Class innerClass)
    {
        EVENTS.put(innerClass, FastList.<String>newList());
        File ouputFile = new File(TableTestUtils.getOutputDirectory(), innerClass.getSimpleName() + ".html");
        if (ouputFile.exists())
        {
            Assert.assertTrue("Deleting " + ouputFile, ouputFile.delete());
        }
        return new JUnitCore().run(innerClass);
    }

    public static class LifecycleTestPassedInnerTest
    {
        @Rule
        public final TableVerifier verifier = newInnerClassVerifier(EVENTS.get(this.getClass()));

        @Test
        public void test()
        {
            this.verifier.verify(Maps.fixedSize.of("table", TableTestUtils.ACTUAL), Maps.fixedSize.of("table", TableTestUtils.ACTUAL));
        }
    }

    public static class LifecycleTestFailedInnerTest
    {
        @Rule
        public final TableVerifier verifier = newInnerClassVerifier(EVENTS.get(this.getClass()));

        @Test
        public void test()
        {
            this.verifier.verify(Maps.fixedSize.of("table", TableTestUtils.ACTUAL), Maps.fixedSize.of("table", TableTestUtils.ACTUAL_2));
        }
    }

    public static class LifecycleTestExceptionInnerTest
    {
        @Rule
        public final TableVerifier verifier = newInnerClassVerifier(EVENTS.get(this.getClass()));

        @Test
        public void test()
        {
            throw new RuntimeException();
        }
    }

    public static class LifecycleTestRebaseInnerTest
    {
        @Rule
        public final TableVerifier verifier = newInnerClassVerifier(EVENTS.get(this.getClass()))
                .withExpectedDir(new File(TableTestUtils.getOutputDirectory(), "expected"))
                .withRebase();

        @Test
        public void test()
        {
            this.verifier.verify("table", TableTestUtils.ACTUAL);
        }
    }

    public static class LifecycleTestRebaseExceptionInnerTest
    {
        @Rule
        public final TableVerifier verifier = newInnerClassVerifier(EVENTS.get(this.getClass()))
                .withExpectedDir(new File(TableTestUtils.getOutputDirectory(), "expected"))
                .withRebase();

        @Test
        public void test()
        {
            throw new RuntimeException();
        }
    }

    public static class LifecycleTestMissingTableInnerTest
    {
        @Rule
        public final TableVerifier verifier = newInnerClassVerifier(EVENTS.get(this.getClass()));

        @Test
        public void test()
        {
            this.verifier.verify(Maps.fixedSize.of("table_1", TableTestUtils.ACTUAL, "table_2", TableTestUtils.ACTUAL_2));
        }
    }

    private static TableVerifier newInnerClassVerifier(final List<String> events)
    {
        return new TableVerifier().withLifecycleEventHandler(new LifecycleEventHandler()
        {
            @Override
            public void onStarted(Description description)
            {
                events.add("onStarted");
            }

            @Override
            public void onSucceeded(Description description)
            {
                events.add("onSucceeded");
            }

            @Override
            public void onFailed(Throwable e, Description description)
            {
                events.add("onFailed");
            }

            @Override
            public void onSkipped(Description description)
            {
                events.add("onSkipped");
            }

            @Override
            public void onFinished(Description description)
            {
                events.add("onFinished");
            }
        })
        .withExpectedDir(TableTestUtils.getExpectedDirectory())
        .withOutputDir(TableTestUtils.getOutputDirectory())
        .withFilePerClass();
    }
}