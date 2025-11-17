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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.*;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.provenance.ProvenanceReporter;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

@SideEffectFree
@Tags({"jsoup, html, xml, json, extract, evaluate"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Evaluates and extracts the html content from the FlowFile body.")
@DynamicProperty(name = "The name of the attribute made from the extracted html element.",
        value = "The CSS property selector of the html element.",
        description = "Each property represents an element that will be extracted from the html contained in the "
                + "FlowFile body. If more elements are found on the same attribute, the output will be a json array.",
        expressionLanguageScope = ExpressionLanguageScope.NONE)
public class EvaluateHtml extends AbstractProcessor {
    public static final PropertyDescriptor SELECT_TEXT = new PropertyDescriptor.Builder()
            .name("Select Element Text")
            .description("Indicates whether to retrieve the full html content of the element or only the text.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .name("Destination")
            .description("Indicates whether to write the elements as attributes or as content.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .allowableValues(Destination.ATTRIBUTE_VALUE, Destination.CONTENT_VALUE)
            .defaultValue(Destination.ATTRIBUTE_VALUE)
            .build();

    public static final PropertyDescriptor NOT_FOUND_BEHAVIOUR = new PropertyDescriptor.Builder()
            .name("Element Not Found Behaviour")
            .description("Indicates how to handle encountering attributes with no matching elements")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .allowableValues(NotFoundBehaviour.WARN_VALUE, NotFoundBehaviour.IGNORE_VALUE, NotFoundBehaviour.SKIP_VALUE)
            .defaultValue(NotFoundBehaviour.WARN_VALUE)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles whose elements have been successfully extracted are routed to this relationship")
            .build();

    public static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not found")
            .description("FlowFiles with no matching elements for the root are sent to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that returned an error are routed to this relationship")
            .build();

    private static final String ATTRIBUTES_CHANGED_DESCRIPTION = "Placed the value of the extracted elements in the "
            + "FlowFile attributes";
    private static final String CONTENT_CHANGED_DESCRIPTION = "Replaced the FlowFile content with the extracted "
            + "elements.";

    private static final Validator CSS_SELECTOR_VALIDATOR = new Validator() {
        private ValidationResult checkSelector(String name, String value) {
            try {
                Jsoup.parse("").select(value);
            } catch (Exception e) {
                return getInvalidResult(name, e.getMessage());
            }
            return getValidResult(name);
        }

        @Override
        public ValidationResult validate(String name, String value, ValidationContext context) {
            if (context.isExpressionLanguagePresent(value)) {
                return getValidResult(name);
            }
            return checkSelector(name, value);
        }
    };

    public static final PropertyDescriptor ROOT_SELECTOR = new PropertyDescriptor.Builder()
            .name("Root Selector")
            .description("Sets the selector of the root element from which to perform the search operation. If the root "
                    + "is not found, the FlowFile is routed to the not found relationship.")
            .required(false)
            .addValidator(CSS_SELECTOR_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            ROOT_SELECTOR,
            SELECT_TEXT,
            DESTINATION,
            NOT_FOUND_BEHAVIOUR
    );

    private final static Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_NOT_FOUND,
            REL_FAILURE
    );

    private static final ObjectMapper mapper = new ObjectMapper();
    private volatile String rootSelector;
    private volatile boolean selectText;
    private volatile String destination;
    private volatile String notFoundBehaviour;
    private volatile List<Property> dynamicProperties;

    private static ValidationResult getValidResult(String name) {
        return new ValidationResult.Builder()
                .subject(name)
                .valid(true)
                .build();
    }

    private static ValidationResult getInvalidResult(String name, String message) {
        return new ValidationResult.Builder()
                .subject(name)
                .valid(false)
                .explanation(message)
                .build();
    }

    @Override
    public Collection<ValidationResult> customValidate(final ValidationContext context) {
        List<ValidationResult> results = new ArrayList<>(super.customValidate(context));

        boolean hasDynamicProperties = context.getProperties().keySet().stream()
                .anyMatch(PropertyDescriptor::isDynamic);

        if (!hasDynamicProperties) {
            ValidationResult validation = getInvalidResult("Dynamic Properties",
                    "At least one dynamic property must be specified");
            results.add(validation);
        }

        return results;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public final Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String name) {
        return new PropertyDescriptor.Builder()
                .name(name)
                .required(false)
                .addValidator(CSS_SELECTOR_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                .dynamic(true)
                .build();
    }

    private List<Property> getDynamicProperties(ProcessContext context) {
        List<Property> properties = new ArrayList<>();
        for (PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
                properties.add(new Property(descriptor.getName(), context.getProperty(descriptor).getValue()));
            }
        }
        return properties;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        rootSelector = context.getProperty(ROOT_SELECTOR).getValue();
        selectText = context.getProperty(SELECT_TEXT).asBoolean();
        destination = context.getProperty(DESTINATION).getValue();
        notFoundBehaviour = context.getProperty(NOT_FOUND_BEHAVIOUR).getValue();
        dynamicProperties = getDynamicProperties(context);
    }

    private String generateContent(Map<String, String> attributes) throws Exception {
        if (dynamicProperties.size() > 1) {
            Collection<String> values = attributes.values();
            return mapper.writeValueAsString(values);
        } else {
            return attributes.values().iterator().next();
        }
    }

    private void resolveFlowFile(FlowFile flowFile, ProcessSession session, Map<String, String> attributes) throws Exception {
        ProvenanceReporter provenance = session.getProvenanceReporter();

        if (destination.equals(Destination.CONTENT)) {
            flowFile = session.putAttribute(flowFile, "mime.type", "application/json");

            String content = generateContent(attributes);
            session.write(flowFile, (out) -> out.write(content.getBytes(StandardCharsets.UTF_8)));
            provenance.modifyContent(flowFile, CONTENT_CHANGED_DESCRIPTION);
        } else {
            session.putAllAttributes(flowFile, attributes);
            provenance.modifyAttributes(flowFile, ATTRIBUTES_CHANGED_DESCRIPTION);
        }
    }

    private String getValue(Element element) {
        if (selectText) {
            return element.text();
        } else {
            return element.outerHtml();
        }
    }

    private String jsonifyElements(Elements elements) throws Exception {
        ArrayNode jsonArray = mapper.createArrayNode();
        for (Element element : elements) {
            jsonArray.add(getValue(element));
        }
        return mapper.writeValueAsString(jsonArray);
    }

    private String processElements(Elements elements) throws Exception {
        if (elements.isEmpty()) {
            return null;
        }

        if (elements.size() > 1) {
            return jsonifyElements(elements);
        } else {
            return getValue(elements.getFirst());
        }
    }

    private void doNotFoundBehaviour(String name, Map<String, String> attributes) {
        switch (notFoundBehaviour) {
            case NotFoundBehaviour.WARN:
                getLogger().warn("Attribute " + name + " did not match any elements.");
            case NotFoundBehaviour.IGNORE:
                attributes.put(name, "");
            case NotFoundBehaviour.SKIP:
        }
    }

    private void extractElements(Element root, Map<String, String> attributes) throws Exception {
        for (Property property : dynamicProperties) {
            Elements matching = root.select(property.value());
            if (matching.isEmpty()) {
                doNotFoundBehaviour(property.name(), attributes);
                return;
            }

            attributes.put(property.name(), processElements(matching));
        }
    }

    private Element extractRootElement(Element html) {
        if (rootSelector == null) {
            return html;
        }

        return html.selectFirst(rootSelector);
    }

    private Element getFlowHtmlContent(ProcessSession session, FlowFile flowFile) throws Exception {
        try (InputStream stream = session.read(flowFile)) {
            return Jsoup.parse(stream, StandardCharsets.UTF_8.name(), "");
        }
    }

    private void processRequest(ProcessContext context, ProcessSession session, FlowFile flowFile) throws Exception {
        Element html = getFlowHtmlContent(session, flowFile);

        Element root = extractRootElement(html);
        if (root == null) {
            session.transfer(flowFile, REL_NOT_FOUND);
            return;
        }

        Map<String, String> attributes = new HashMap<>();
        extractElements(root, attributes);
        resolveFlowFile(flowFile, session, attributes);

        session.transfer(flowFile, REL_SUCCESS);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) {
        FlowFile original = session.get();
        if (original == null) {
            return;
        }

        try {
            processRequest(context, session, original);
        } catch (Exception e) {
            getLogger().error(e.getMessage());
            session.transfer(original, REL_FAILURE);
        }
    }

    private record Property(String name, String value) {
    }

    public static class Destination {
        private static final String CONTENT = "FlowFile-Content";
        public static final AllowableValue CONTENT_VALUE =
                new AllowableValue(CONTENT, CONTENT, "Write the elements in the FlowFile content. If more " +
                        "than one property is specified, the elements will be written as a JSON array ");

        private static final String ATTRIBUTE = "FlowFile-Attribute";
        public static final AllowableValue ATTRIBUTE_VALUE =
                new AllowableValue(ATTRIBUTE, ATTRIBUTE, "Write the elements as attributes");
    }

    public static class NotFoundBehaviour {
        private static final String WARN = "Warn";
        public static final AllowableValue WARN_VALUE =
                new AllowableValue(WARN, WARN, "Trigger a warning and ignore this attribute.");

        private static final String IGNORE = "Ignore";
        public static final AllowableValue IGNORE_VALUE =
                new AllowableValue(IGNORE, IGNORE, "Set the attribute as empty.");

        private static final String SKIP = "Skip";
        public static final AllowableValue SKIP_VALUE =
                new AllowableValue(SKIP, SKIP, "Excludes this attribute from the final result.");
    }
}
