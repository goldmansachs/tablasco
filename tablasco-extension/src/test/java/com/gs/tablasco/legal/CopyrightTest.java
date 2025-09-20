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

package com.gs.tablasco.legal;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class CopyrightTest {
    @Test
    void test() throws IOException {
        List<File> javaSource = new ArrayList<>();
        scan(new File("src"), javaSource);
        assertFalse(javaSource.isEmpty());
        for (File file : javaSource) {
            List<String> lines = Files.readAllLines(file.toPath(), Charset.defaultCharset());
            boolean foundApacheLicence = false;
            boolean foundApacheUrl = false;
            for (String line : lines) {
                if (line.contains("Licensed under the Apache License, Version 2.0")) {
                    foundApacheLicence = true;
                }
                if (line.contains("http://www.apache.org/licenses/LICENSE-2.0")) {
                    foundApacheUrl = true;
                }
            }
            assertTrue(foundApacheLicence, "Found Apache license in " + file.getName());
            assertTrue(foundApacheUrl, "Found Apache license URL in " + file.getName());
        }
    }

    private void scan(File dir, List<File> javaSource) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    scan(file, javaSource);
                } else if (file.getName().endsWith(".java")) {
                    javaSource.add(file);
                }
            }
        }
    }
}
