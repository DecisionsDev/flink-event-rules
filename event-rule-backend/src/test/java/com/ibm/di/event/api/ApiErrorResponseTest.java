/*
 *
 *   Copyright IBM Corp. 2026
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */
package com.ibm.di.event.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit and regression tests for {@link ApiErrorResponse}.
 */
@DisplayName("ApiErrorResponse Unit Tests")
class ApiErrorResponseTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    @DisplayName("Factory badRequest sets fields correctly")
    void badRequestFactory() {
        ApiErrorResponse res = ApiErrorResponse.badRequest("INVALID_PARAM", "Parameter is invalid", "req-123");
        assertEquals(400, res.getStatus());
        assertEquals("Bad Request", res.getError());
        assertEquals("INVALID_PARAM", res.getCode());
        assertEquals("Parameter is invalid", res.getMessage());
        assertEquals("req-123", res.getRequestId());
    }

    @Test
    @DisplayName("Factory notFound sets fields correctly")
    void notFoundFactory() {
        ApiErrorResponse res = ApiErrorResponse.notFound("req-456");
        assertEquals(404, res.getStatus());
        assertEquals("Not Found", res.getError());
        assertEquals("NOT_FOUND", res.getCode());
        assertEquals("The requested path does not exist", res.getMessage());
        assertEquals("req-456", res.getRequestId());
    }

    @Test
    @DisplayName("Factory methodNotAllowed sets fields correctly")
    void methodNotAllowedFactory() {
        ApiErrorResponse res = ApiErrorResponse.methodNotAllowed("Only POST allowed", "req-789");
        assertEquals(405, res.getStatus());
        assertEquals("Method Not Allowed", res.getError());
        assertEquals("METHOD_NOT_ALLOWED", res.getCode());
        assertEquals("Only POST allowed", res.getMessage());
        assertEquals("req-789", res.getRequestId());
    }

    @Test
    @DisplayName("Factory unsupportedMediaType sets fields correctly")
    void unsupportedMediaTypeFactory() {
        ApiErrorResponse res = ApiErrorResponse.unsupportedMediaType("req-000");
        assertEquals(415, res.getStatus());
        assertEquals("Unsupported Media Type", res.getError());
        assertEquals("UNSUPPORTED_MEDIA_TYPE", res.getCode());
        assertEquals("Content-Type must be application/json", res.getMessage());
        assertEquals("req-000", res.getRequestId());
    }

    @Test
    @DisplayName("Factory internal sets fields correctly")
    void internalFactory() {
        ApiErrorResponse res = ApiErrorResponse.internal("Kafka unreachable", "req-999");
        assertEquals(500, res.getStatus());
        assertEquals("Internal Server Error", res.getError());
        assertEquals("INTERNAL_ERROR", res.getCode());
        assertEquals("Kafka unreachable", res.getMessage());
        assertEquals("req-999", res.getRequestId());
    }

    @Test
    @DisplayName("Serialization to JSON preserves all fields")
    void jsonSerializationRoundTrip() throws Exception {
        ApiErrorResponse res = new ApiErrorResponse(422, "Unprocessable Entity", "SEMANTIC_ERROR", "Validation failed", "req-custom");
        String json = mapper.writeValueAsString(res);

        assertTrue(json.contains("\"status\":422"));
        assertTrue(json.contains("\"error\":\"Unprocessable Entity\""));
        assertTrue(json.contains("\"code\":\"SEMANTIC_ERROR\""));
        assertTrue(json.contains("\"message\":\"Validation failed\""));
        assertTrue(json.contains("\"requestId\":\"req-custom\""));
    }
}
