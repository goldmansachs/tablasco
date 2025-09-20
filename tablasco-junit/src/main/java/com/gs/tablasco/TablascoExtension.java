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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class TablascoExtension implements BeforeEachCallback, AfterEachCallback {
    @Override
    public void beforeEach(ExtensionContext context) {
        Description description = getDescription(context);
        TableVerifier tableVerifier = getTableVerifier(context, description);
        tableVerifier.starting(description);
    }

    @Override
    public void afterEach(ExtensionContext context) {
        Description description = getDescription(context);
        TableVerifier tableVerifier = getTableVerifier(context, description);
        Optional<Throwable> executionException = context.getExecutionException();
        if (executionException.isEmpty()) {
            tableVerifier.succeeded(description);
        }
    }

    private static Description getDescription(ExtensionContext context) {
        Class<?> testClass = context.getRequiredTestClass();
        Method testMethod = context.getRequiredTestMethod();
        return new Description(testClass, testMethod);
    }

    private static TableVerifier getTableVerifier(ExtensionContext context, Description description) {
        return Arrays.stream(description.getTestClass().getDeclaredFields())
                .filter(field -> field.getType().equals(TableVerifier.class))
                .map(field -> {
                    try {
                        field.setAccessible(true);
                        return (TableVerifier) field.get(context.getRequiredTestInstance());
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException("Failed to set description on Tablasco instance", e);
                    }
                })
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No TableVerifier field found"));
    }
}
