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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gs.tablasco.lifecycle.LifecycleEventHandler;
import java.io.File;
import java.util.*;
import org.junit.Rule;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

@SuppressWarnings("ALL")
public class LifecycleTest {
    private static final Map<Class, List<String>> EVENTS = new HashMap<>();
    public static final String INNER_TEST_EXECUTION = "inner.test.execution";

    // todo: rebase, missing table, rebase exception

    @Test
    void passedTest() {
        Class testClass = LifecycleTestPassedInnerTest.class;
        Result result = runInnerTest(testClass);
        assertTrue(result.wasSuccessful());
        assertTrue(getOutputFile(testClass).exists());
        assertEquals(Arrays.asList("onStarted", "onSucceeded", "onFinished"), EVENTS.get(testClass));
    }

    @Test
    void failedTest() {
        Class testClass = LifecycleTestFailedInnerTest.class;
        Result result = runInnerTest(testClass);
        assertFalse(result.wasSuccessful());
        assertTrue(getOutputFile(testClass).exists());
        assertEquals(Arrays.asList("onStarted", "onFailed", "onFinished"), EVENTS.get(testClass));
    }

    @Test
    void exceptionTest() {
        Class testClass = LifecycleTestExceptionInnerTest.class;
        Result result = runInnerTest(testClass);
        assertFalse(result.wasSuccessful());
        assertTrue(getOutputFile(testClass).exists());
        assertEquals(Arrays.asList("onStarted", "onFailed", "onFinished"), EVENTS.get(testClass));
    }

    @Test
    void rebaseTest() {
        Class testClass = LifecycleTestRebaseInnerTest.class;
        Result result = runInnerTest(testClass);
        assertFalse(result.wasSuccessful());
        assertFalse(getOutputFile(testClass).exists());
        assertTrue(
                new File(new File(TableTestUtils.getOutputDirectory(), "expected"), testClass.getSimpleName() + ".txt")
                        .exists());
        assertEquals(Arrays.asList("onStarted", "onSucceeded", "onFinished"), EVENTS.get(testClass));
    }

    @Test
    void rebaseExceptionTest() {
        Class testClass = LifecycleTestRebaseExceptionInnerTest.class;
        Result result = runInnerTest(testClass);
        assertFalse(result.wasSuccessful());
        assertTrue(getOutputFile(testClass).exists());
        assertFalse(
                new File(new File(TableTestUtils.getOutputDirectory(), "expected"), testClass.getSimpleName() + ".txt")
                        .exists());
        assertEquals(Arrays.asList("onStarted", "onFailed", "onFinished"), EVENTS.get(testClass));
    }

    @Test
    void missingTableTest() {
        Class testClass = LifecycleTestMissingTableInnerTest.class;
        Result result = runInnerTest(testClass);
        assertFalse(result.wasSuccessful());
        assertTrue(getOutputFile(testClass).exists());
        assertEquals(Arrays.asList("onStarted", "onFailed", "onFinished"), EVENTS.get(testClass));
    }

    private static File getOutputFile(Class innerClass) {
        return new File(TableTestUtils.getOutputDirectory(), innerClass.getSimpleName() + ".html");
    }

    private static Result runInnerTest(Class innerClass) {
        EVENTS.put(innerClass, new ArrayList<>());
        File ouputFile = new File(TableTestUtils.getOutputDirectory(), innerClass.getSimpleName() + ".html");
        if (ouputFile.exists()) {
            assertTrue(ouputFile.delete(), "Deleting " + ouputFile);
        }
        try {
            System.setProperty(INNER_TEST_EXECUTION, Boolean.TRUE.toString());
            return new JUnitCore().run(innerClass);
        } finally {
            System.clearProperty(INNER_TEST_EXECUTION);
        }
    }

    private static void assumeInnerTestExecution() {
        Assumptions.assumeTrue(Boolean.getBoolean(INNER_TEST_EXECUTION));
    }

    @Nested
    public class LifecycleTestPassedInnerTest {
        @Rule
        public final TableVerifier verifier = newInnerClassVerifier(EVENTS.get(this.getClass()));

        @Test
        void test() {
            assumeInnerTestExecution();
            this.verifier.verify("table", TableTestUtils.ACTUAL, TableTestUtils.ACTUAL);
        }
    }

    @Nested
    public class LifecycleTestFailedInnerTest {
        @Rule
        public final TableVerifier verifier = newInnerClassVerifier(EVENTS.get(this.getClass()));

        @Test
        void test() {
            assumeInnerTestExecution();
            this.verifier.verify("table", TableTestUtils.ACTUAL, TableTestUtils.ACTUAL_2);
        }
    }

    @Nested
    public class LifecycleTestExceptionInnerTest {
        @Rule
        public final TableVerifier verifier = newInnerClassVerifier(EVENTS.get(this.getClass()));

        @Test
        void test() {
            assumeInnerTestExecution();
            throw new RuntimeException();
        }
    }

    @Nested
    public class LifecycleTestRebaseInnerTest {
        @Rule
        public final TableVerifier verifier = newInnerClassVerifier(EVENTS.get(this.getClass()))
                .withExpectedDir(new File(TableTestUtils.getOutputDirectory(), "expected"))
                .withRebase();

        @Test
        void test() {
            assumeInnerTestExecution();
            this.verifier.verify("table", TableTestUtils.ACTUAL);
        }
    }

    @Nested
    public class LifecycleTestRebaseExceptionInnerTest {
        @Rule
        public final TableVerifier verifier = newInnerClassVerifier(EVENTS.get(this.getClass()))
                .withExpectedDir(new File(TableTestUtils.getOutputDirectory(), "expected"))
                .withRebase();

        @Test
        void test() {
            assumeInnerTestExecution();
            throw new RuntimeException();
        }
    }

    @Nested
    public class LifecycleTestMissingTableInnerTest {
        @Rule
        public final TableVerifier verifier = newInnerClassVerifier(EVENTS.get(this.getClass()));

        @Test
        void test() {
            assumeInnerTestExecution();
            this.verifier.verify(
                    TableTestUtils.toNamedTables("table_1", TableTestUtils.ACTUAL, "table_2", TableTestUtils.ACTUAL_2));
        }
    }

    private static TableVerifier newInnerClassVerifier(final List<String> events) {
        return new TableVerifier()
                .withLifecycleEventHandler(new LifecycleEventHandler() {
                    @Override
                    public void onStarted(Description description) {
                        events.add("onStarted");
                    }

                    @Override
                    public void onSucceeded(Description description) {
                        events.add("onSucceeded");
                    }

                    @Override
                    public void onFailed(Throwable e, Description description) {
                        events.add("onFailed");
                    }

                    @Override
                    public void onSkipped(Description description) {
                        events.add("onSkipped");
                    }

                    @Override
                    public void onFinished(Description description) {
                        events.add("onFinished");
                    }
                })
                .withExpectedDir(TableTestUtils.getExpectedDirectory())
                .withOutputDir(TableTestUtils.getOutputDirectory())
                .withFilePerClass();
    }
}
