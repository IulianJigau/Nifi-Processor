/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.globaldatabase.processors.html;

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ProcessorTest {

    private static final Logger logger = LoggerFactory.getLogger(ProcessorTest.class);
    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(EvaluateHtml.class);
    }

    @Test
    public void testProcessor() {
        testRunner.setProperty(EvaluateHtml.ROOT_SELECTOR, "//header[@id='main-header']");
        testRunner.setProperty(EvaluateHtml.SELECT_TEXT, "true");
        testRunner.setProperty(EvaluateHtml.DESTINATION, EvaluateHtml.Destination.CONTENT_VALUE.getValue());
        testRunner.setProperty(EvaluateHtml.NOT_FOUND_BEHAVIOUR, EvaluateHtml.NotFoundBehaviour.WARN_VALUE.getValue());
        testRunner.setProperty(EvaluateHtml.USE_XPATH, "true");
        testRunner.setProperty("list", ".//li");

        File file = new File("src/test/resources/test.html");
        try (InputStream inputStream = new FileInputStream(file)) {
            testRunner.enqueue(inputStream);
        } catch (Exception e) {
            logger.error("Failed to import the test file", e);
        }

        testRunner.run();

        for (Relationship rel : testRunner.getProcessor().getRelationships()) {
            List<MockFlowFile> files = testRunner.getFlowFilesForRelationship(rel);
            if (files.isEmpty()) continue;

            System.out.println("RELATIONSHIP: " + rel.getName());
            for (MockFlowFile ff : files) {
                System.out.println("FlowFile ID: " + ff.getAttribute("uuid"));

                ff.getAttributes().forEach((k, v) -> System.out.println("  " + k + " = " + v));

                try {
                    String content = new String(ff.toByteArray(), StandardCharsets.UTF_8);
                    System.out.println("Content:");
                    System.out.println(content);
                } catch (Exception e) {
                    logger.error("Failed to read FlowFile content", e);
                }

                System.out.println();
            }
        }
    }

}
