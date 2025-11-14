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
import java.util.HashMap;
import java.util.Map;

public class ProcessorTest {

    private static final Logger logger = LoggerFactory.getLogger(ProcessorTest.class);
    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(EvaluateHtml.class);
    }

    @Test
    public void testProcessor() {
//        testRunner.setProperty(EvaluateHtml.ROOT_SELECTOR, "");
        testRunner.setProperty(EvaluateHtml.SELECT_MULTIPLE, "true");
        testRunner.setProperty(EvaluateHtml.SELECT_TEXT, "true");
        testRunner.setProperty(EvaluateHtml.DESTINATION, EvaluateHtml.Destination.CONTENT_VALUE.getValue());
        testRunner.setProperty(EvaluateHtml.NOT_FOUND_BEHAVIOUR, EvaluateHtml.NotFoundBehaviour.WARN_VALUE.getValue());
        testRunner.setProperty("home", "#headerhome");

        File file = new File("src/test/resources/test.html");
        try (InputStream inputStream = new FileInputStream(file)) {
            Map<String, String> attributes = new HashMap<>();
            testRunner.enqueue(inputStream, attributes);
        } catch (Exception e) {
            logger.error("Failed to import the test file", e);
        }

        testRunner.run();

        for (MockFlowFile flowFile : testRunner.getFlowFilesForRelationship(EvaluateHtml.REL_SUCCESS)) {
            System.out.println("Attributes:");
            flowFile.getAttributes().forEach((k, v) -> System.out.println("  " + k + " = " + v));

            System.out.println("Content:");
            try {
                String content = new String(flowFile.toByteArray(), StandardCharsets.UTF_8);
                System.out.println(content);
            } catch (Exception e) {
                logger.error("Failed to read FlowFile content", e);
            }
        }
    }
}
