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

class Description {
    private final Class<?> testClass;
    private final Method testMethod;

    Description(Class<?> testClass, Method testMethod) {
        this.testClass = testClass;
        this.testMethod = testMethod;
    }

    Class<?> getTestClass() {
        return this.testClass;
    }

    String getMethodName() {
        return this.testMethod.getName();
    }
}
