/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.ObjectKey;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KeyGeneratorTest {
    @Mock
    private S3SinkConfig s3SinkConfig;

    @Mock
    private ExtensionProvider extensionProvider;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @BeforeEach
    void setUp() {

    }

    private KeyGenerator createObjectUnderTest() {
        return new KeyGenerator(s3SinkConfig, extensionProvider, expressionEvaluator);
    }

    @Test
    void test_generateKey_with_date_prefix() {
        String pathPrefix = "logdata/";
        final String objectName = UUID.randomUUID().toString();
        when(extensionProvider.getExtension()).thenReturn(null);

        final KeyGenerator objectUnderTest = createObjectUnderTest();

        final Event event = mock(Event.class);

        try (final MockedStatic<ObjectKey> objectKeyMockedStatic = mockStatic(ObjectKey.class)) {

            objectKeyMockedStatic.when(() -> ObjectKey.buildingPathPrefix(s3SinkConfig, event, expressionEvaluator))
                    .thenReturn(pathPrefix);
            objectKeyMockedStatic.when(() -> ObjectKey.objectFileName(s3SinkConfig, null, event, expressionEvaluator))
                    .thenReturn(objectName);

            String key = objectUnderTest.generateKeyForEvent(event);
            assertNotNull(key);
            assertThat(key, true);
            assertThat(key.contains(pathPrefix), equalTo(true));
            assertThat(key.contains(objectName), equalTo(true));
        }
    }

    @Test
    void generateKey_with_extension() {
        String extension = UUID.randomUUID().toString();
        final String objectName = UUID.randomUUID().toString();
        when(extensionProvider.getExtension()).thenReturn(extension);
        String pathPrefix = "events/";

        final Event event = mock(Event.class);
        final KeyGenerator objectUnderTest = createObjectUnderTest();
        try (final MockedStatic<ObjectKey> objectKeyMockedStatic = mockStatic(ObjectKey.class)) {

            objectKeyMockedStatic.when(() -> ObjectKey.buildingPathPrefix(s3SinkConfig, event, expressionEvaluator))
                    .thenReturn(pathPrefix);
            objectKeyMockedStatic.when(() -> ObjectKey.objectFileName(s3SinkConfig, extension, event, expressionEvaluator))
                    .thenReturn(objectName);

            String key = objectUnderTest.generateKeyForEvent(event);
            assertThat(key, notNullValue());
            assertThat(key.contains(pathPrefix), equalTo(true));
            assertThat(key.contains(objectName), equalTo(true));
        }

    }
}