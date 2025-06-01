package com.example.NMS.utility;

import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Utility class for validating JSON request fields in API endpoints.
 */
public class Validator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Validator.class);

    /**
     * Checks if required fields are present in the JSON request body.
     *
     * @param context            The routing context containing the HTTP request.
     * @param fields         Array of field names to check in the JSON body.
     * @param allMustExist   If true, all fields must be present and non-empty; if false, at least one field must be present and non-empty.
     * @return true if validation fails (and an error response is sent), false if validation passes.
     */
    public static boolean checkRequestFields(RoutingContext context, String[] fields, boolean allMustExist)
    {
        try
        {
            var body = context.body().asJsonObject();

            if (body == null)
            {
                LOGGER.warn("Request validation failed: No valid JSON body provided");

                APIUtils.sendError(context, 400, "Invalid or missing JSON body");

                return true;
            }

            String[] missing = Arrays.stream(fields)
                .filter(field -> {
                    var value = body.getValue(field);
                    return value == null || (value instanceof String && ((String) value).isEmpty());
                })
                .toArray(String[]::new);

            boolean allMissing = missing.length == fields.length;

            if (allMustExist && missing.length > 0)
            {
                var errorMsg = "Required fields missing: " + String.join(", ", missing);

                LOGGER.warn("Request validation failed: {}", errorMsg);

                APIUtils.sendError(context, 400, errorMsg);

                return true;
            }

            if (!allMustExist && allMissing)
            {
                var errorMsg = "At least one field required: " + String.join(", ", fields);

                LOGGER.warn("Request validation failed: {}", errorMsg);

                APIUtils.sendError(context, 400, errorMsg);

                return true;
            }

            return false;
        }
        catch (Exception exception)
        {
            LOGGER.error("Request validation failed due to invalid JSON: {}", exception.getMessage());

            APIUtils.sendError(context, 400, "Malformed JSON input");

            return true;
        }
    }
}
