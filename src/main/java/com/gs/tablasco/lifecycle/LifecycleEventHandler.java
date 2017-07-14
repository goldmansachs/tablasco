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

package com.gs.tablasco.lifecycle;

import org.junit.runner.Description;

public interface LifecycleEventHandler
{
    void onStarted(Description description);

    void onSucceeded(Description description);

    void onFailed(Throwable e, Description description);

    void onSkipped(Description description);

    void onFinished(Description description);
}
