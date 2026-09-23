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

/**
 * Uniform JSON error body returned by all API endpoints on 4xx and 5xx responses.
 *
 * <p>Shape:
 * <pre>
 * {
 *   "status":    400,
 *   "error":     "Bad Request",
 *   "code":      "MISSING_VERSION",
 *   "message":   "Field 'version' is required and must not be blank",
 *   "requestId": "deploy-20250709-01"
 * }
 * </pre>
 *
 * <p>{@code requestId} is {@code null} when the request body could not be parsed at all.
 */
public final class ApiErrorResponse {

    private final int    status;
    private final String error;
    private final String code;
    private final String message;
    private final String requestId;

    public ApiErrorResponse(int status, String error, String code,
                            String message, String requestId) {
        this.status    = status;
        this.error     = error;
        this.code      = code;
        this.message   = message;
        this.requestId = requestId;
    }

    // ── Convenience factories ─────────────────────────────────────────────────

    public static ApiErrorResponse badRequest(String code, String message, String requestId) {
        return new ApiErrorResponse(400, "Bad Request", code, message, requestId);
    }

    public static ApiErrorResponse notFound(String requestId) {
        return new ApiErrorResponse(404, "Not Found", "NOT_FOUND",
            "The requested path does not exist", requestId);
    }

    public static ApiErrorResponse methodNotAllowed(String hint, String requestId) {
        return new ApiErrorResponse(405, "Method Not Allowed", "METHOD_NOT_ALLOWED",
            hint, requestId);
    }

    public static ApiErrorResponse unsupportedMediaType(String requestId) {
        return new ApiErrorResponse(415, "Unsupported Media Type", "UNSUPPORTED_MEDIA_TYPE",
            "Content-Type must be application/json", requestId);
    }

    public static ApiErrorResponse internal(String detail, String requestId) {
        return new ApiErrorResponse(500, "Internal Server Error", "INTERNAL_ERROR",
            detail, requestId);
    }

    // ── Getters (required by Jackson serialisation) ───────────────────────────

    public int    getStatus()    { return status; }
    public String getError()     { return error; }
    public String getCode()      { return code; }
    public String getMessage()   { return message; }
    public String getRequestId() { return requestId; }
}

