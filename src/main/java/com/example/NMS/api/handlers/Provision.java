package com.example.NMS.api.handlers;

import com.example.NMS.cache.MetricCache;
import com.example.NMS.constant.QueryConstant;
//import com.example.NMS.service.ProvisionService;
import com.example.NMS.utility.APIUtils;
import com.example.NMS.utility.Validator;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.example.NMS.constant.Constant.*;
import static com.example.NMS.constant.QueryConstant.*;
import static com.example.NMS.constant.QueryConstant.GET_PROVISIONING_JOB_AND_METRICS;
import static com.example.NMS.utility.DBUtils.*;

/**
 * Manages provisioning jobs in Lite NMS, handling creation, retrieval, deletion, metric updates, and polled data retrieval.
 * This class provides REST-ful API endpoints to manage provisioning jobs, which define network devices to be monitored,
 * including their IP addresses, ports, and associated metrics.
 */
public class Provision extends AbstractAPI
{
    public static final Logger LOGGER = LoggerFactory.getLogger(Provision.class);

    public Provision()
    {
        super(LOGGER, "Provisioning Jobs");
    }
    /**
     * Initializes API routes for provisioning job management endpoints.
     * Sets up routes for creating, retrieving, deleting, updating metrics, and fetching polled data for provisioning jobs.
     *
     * @param provisionRouter The Vert.x router to attach the provisioning endpoints to.
     */
    public void init(Router provisionRouter)
    {
        provisionRouter.post("/api/provision/:id").handler(this::create);

//        provisionRouter.get("/api/provision").handler(this::getAll);
//
//        provisionRouter.get("/api/provision/:id").handler(this::getById);

        provisionRouter.get("/api/provision").handler(ctx ->
            super.getAll(ctx, QueryConstant.GET_ALL_PROVISIONING_JOBS)
        );
        provisionRouter.get("/api/provision/:id").handler(ctx ->
            super.getById(ctx, QueryConstant.GET_PROVISIONING_JOB_BY_ID)
        );

        provisionRouter.delete("/api/provision/:id").handler(this::delete);

        provisionRouter.put("/api/provision/:id/metrics").handler(this::update);

        provisionRouter.get("/api/polled-data").handler(this::getAllPolledData);
    }


    /**
     * Handles POST requests to create provisioning jobs for a device.
     * Validates the discovery profile ID and selected IP addresses, then creates provisioning jobs for monitoring.
     *
     * @param context The routing context containing the HTTP request with discovery profile ID and IP addresses.
     */
    public void create(RoutingContext context)
    {
        try
        {
            var discoveryId = APIUtils.parseIdFromPath(context, ID);

            if (discoveryId == -1) return;

            var fields = new String[]{SELECTED_IPS};

            if (Validator.checkRequestFields(context, fields, true))
            {
                return;
            }

            var body = context.body().asJsonObject();

            var selectedIps = body.getJsonArray(SELECTED_IPS);

            var query = new JsonObject()
                .put(QUERY, QueryConstant.INSERT_PROVISIONING_AND_METRICS)
                .put(PARAMS, new JsonArray().add(discoveryId).add(selectedIps));

            executeQuery(query).onComplete(res ->
            {
                if (res.failed())
                {
                    APIUtils.sendError(context, 500, "Provisioning failed: " + res.cause().getMessage());

                    return;
                }

                var result = res.result();

                if (result.isEmpty())
                {
                    APIUtils.sendError(context, 400, "No provisioning jobs created");

                    return;
                }

                var row = result.getJsonObject(0);

                var records = row.getJsonArray("records", new JsonArray());

                var validIps = row.getJsonArray("valid_ips", new JsonArray());

                var invalidIps = row.getJsonArray("invalid_ips", new JsonArray());

                if (records.isEmpty())
                {
                    APIUtils.sendError(context, 400, "No valid IPs for provisioning: " + invalidIps.encode());

                    return;
                }

                var insertedRecords = new JsonArray();

                for (var entry : records)
                {
                    var record = (JsonObject) entry;

                    var pollingInterval = 300;

                    // Create cacheObject by copying the record and adding extra fields
                    var cacheObject = new JsonObject(record.getMap())
                        .put(ORIGINAL_INTERVAL, pollingInterval)
                        .put(REMAINING_TIME, pollingInterval);

                    MetricCache.getInstance().insert(cacheObject);

                    // Add to insertedRecords
                    insertedRecords.add(new JsonObject()
                        .put("ip", record.getString(IP))
                        .put("status", "created")
                        .put(PROVISIONING_JOB_ID, record.getLong(PROVISIONING_JOB_ID))
                        .put(METRIC_ID, record.getLong(METRIC_ID))
                        .put(METRIC_NAME, record.getString(METRIC_NAME)));
                }

                var response = new JsonObject()
                    .put("validIps", validIps)
                    .put("invalidIps", invalidIps)
                    .put("insertedRecords", insertedRecords);

                APIUtils.sendSuccess(context, 201, "Provisioning successful", new JsonArray().add(response));
            });
        }
        catch (Exception exception)
        {
            LOGGER.error(exception.getMessage());

            APIUtils.sendError(context, 500, "Internal server error");
        }
    }

    /**
     * Handles GET requests to retrieve all provisioning jobs.
     * Fetches all provisioning jobs from the database and returns them.
     *
     * @param context The routing context containing the HTTP request.
     */
    public void getAll(RoutingContext context)
    {
        try
        {
            // Prepare query to fetch all provisioning jobs
            var query = new JsonObject()
                .put(QUERY, GET_ALL_PROVISIONING_JOBS);

            executeQuery(query)
                .onComplete(queryResult ->
                {
                    if(queryResult.succeeded())
                    {
                        var result = queryResult.result();

                        if (result.isEmpty())
                        {
                            APIUtils.sendError(context, 404, "No provisioning jobs found");

                            return;
                        }

                        APIUtils.sendSuccess(context,200, "all provision",result);
                    }
                    else
                    {
                        var error = queryResult.cause();

                        APIUtils.sendError(context, 500, "Database query failed: " + error.getMessage());
                    }
                });
        }
        catch (Exception exception)
        {
            LOGGER.error("Error getting all provisions: {}", exception.getMessage());

            APIUtils.sendError(context, 500, "Internal server error");
        }
    }

    /**
     * Handles DELETE requests to remove a provisioning job by its ID.
     * Deletes the job from the database and removes associated metrics from the cache.
     *
     * @param context The routing context containing the HTTP request with provisioning job ID.
     */
    public void delete(RoutingContext context)
    {
        try
        {
            // Parse and validate provisioning job ID from path
            var id = APIUtils.parseIdFromPath(context, ID);

            if (id == -1)
            {
                return;
            }

            var query = new JsonObject()
                .put(QUERY, DELETE_PROVISIONING_JOB)
                .put(PARAMS, new JsonArray().add(id));

            executeQuery(query)
                .onComplete(queryResult ->
                {
                    if(queryResult.succeeded())
                    {
                        var result = queryResult.result();

                        if (!result.isEmpty())
                        {
                            MetricCache.getInstance().delete(id);

                            APIUtils.sendSuccess(context,200, "Provision deleted successfully" ,result);

                        }
                        else
                        {
                            APIUtils.sendError(context, 404, "Provisioning job not found");
                        }
                    }
                    else
                    {
                        var error = queryResult.cause();

                        APIUtils.sendError(context, 500, "Database query failed: " + error.getMessage());
                    }
                });
        }
        catch (Exception exception)
        {
            LOGGER.error("Error deleting provision job: {}", exception.getMessage());

            APIUtils.sendError(context, 500, "Internal server error");
        }
    }

    /**
     * Handles PUT requests to update metrics for a provisioning job.
     * Validates the metrics configuration, upsert metrics in the database, and updates the metric cache.
     *
     * @param context The routing context containing the HTTP request with provisioning job ID and metrics data.
     */
    public void update(RoutingContext context)
    {
        try
        {
            var id = APIUtils.parseIdFromPath(context, ID);

            if (id == -1) return;

            var fields = new String[]{"metrics"};

            if (Validator.checkRequestFields(context, fields, true))
            {
                return;
            }

            var body = context.body().asJsonObject();

            var metrics = body.getJsonArray("metrics");

            // Build batch parameters and validate metrics
            var batchParams = new JsonArray();

            for (var i = 0; i < metrics.size(); i++)
            {
                var metric = metrics.getJsonObject(i);

                var name = metric.getString(METRIC_NAME);

                var isEnabled = metric.getBoolean(IS_ENABLED);

                if (name == null || isEnabled == null)
                {
                    APIUtils.sendError(context, 400, "Invalid metric configuration: metric_name and is_enabled are required");

                    return;
                }

                var interval = metric.getInteger(POLLING_INTERVAL);

                if (isEnabled && (interval == null || interval <= 0))
                {
                    APIUtils.sendError(context, 400, "Invalid metric configuration: polling_interval must be positive when is_enabled is true");

                    return;
                }

                int effectiveInterval = (interval != null && interval > 0) ? interval : 300;

                batchParams.add(new JsonArray().add(id).add(name).add(effectiveInterval).add(isEnabled));
            }

            // Upsert metrics and fetch updated data in one query
            var upsertQuery = new JsonObject()
                .put(QUERY, QueryConstant.UPSERT_METRICS)
                .put(BATCHPARAMS, batchParams);

            executeBatchQuery(upsertQuery).compose(updatedMetrics ->
            {
                // Fetch provisioning job details and updated metrics in one query
                var fetchQuery = new JsonObject()
                    .put(QUERY,GET_PROVISIONING_JOB_AND_METRICS)
                    .put(PARAMS, new JsonArray().add(id).add(new JsonArray(metrics.stream()
                        .map(m -> ((JsonObject) m).getString(METRIC_NAME))
                        .toList())));

                return executeQuery(fetchQuery);
            }).onComplete(result ->
            {
                if (result.failed())
                {
                    var error = result.cause();

                    APIUtils.sendError(context, error.getMessage().contains("not found") ? 404 : 500, "Failed to update metrics: " + error.getMessage());

                    return;
                }

                var updatedData = result.result();

                if (updatedData.isEmpty())
                {
                    APIUtils.sendError(context, 404, "Provisioning job or metrics not found");

                    return;
                }

                // Update MetricCache for all updated metrics
                for (var i = 0; i < updatedData.size(); i++)
                {
                    var row = updatedData.getJsonObject(i);

                    var cacheObject = new JsonObject(row.getMap())
                        .put(PROVISIONING_JOB_ID, id)
                        .put(METRIC_ID, row.getLong(METRIC_ID))
                        .put(METRIC_NAME, row.getString(METRIC_NAME))
                        .put(CRED_DATA, row.getJsonObject(CRED_DATA, new JsonObject()))
                        .put(ORIGINAL_INTERVAL, row.getInteger(POLLING_INTERVAL))
                        .put(REMAINING_TIME, row.getInteger(POLLING_INTERVAL))
                        .put(IS_ENABLED, row.getBoolean(IS_ENABLED));

                    MetricCache.getInstance().update(cacheObject);
                }

                APIUtils.sendSuccess(context, 200, "Updated metrics successfully", new JsonArray().add(id));
            });
        }
        catch (Exception exception)
        {
            LOGGER.error("Error updating metrics: {}", exception.getMessage());

            APIUtils.sendError(context, 500, "Internal server error");
        }
    }

    private void getById(RoutingContext context)
    {
        try
        {
            // Parse and validate credential ID from path parameter
            var id = APIUtils.parseIdFromPath(context, ID);

            if (id == -1)
            {
                return;
            }

            // Prepare query to fetch credential by ID
            var getQuery = new JsonObject()
                .put(QUERY, GET_PROVISIONING_JOB_BY_ID)
                .put(PARAMS, new JsonArray().add(id));

            executeQuery(getQuery)
                .onComplete(queryResult ->
                {
                    if (queryResult.succeeded())
                    {
                        var result = queryResult.result();

                        if (!result.isEmpty())
                        {
                            APIUtils.sendSuccess(context, 200, "provision profile for current Id",result);
                        }
                        else
                        {
                            APIUtils.sendError(context, 404, "provision not found");
                        }
                    }
                    else
                    {
                        var error = queryResult.cause();

                        LOGGER.error("Failed to fetch provision {}: {}", id, error.getMessage());

                        APIUtils.sendError(context, 500, "Database error: " + error.getMessage());
                    }
                });
        }
        catch (Exception exception)
        {
            LOGGER.error("Unexpected error while fetching provision: {}", exception.getMessage());

            APIUtils.sendError(context, 500, "Unexpected error: " + exception.getMessage());
        }
    }


    /**
     * Handles GET requests to retrieve all polled data for provisioning jobs.
     * Fetches polled data from the database and returns it.
     *
     * @param context The routing context containing the HTTP request.
     */
    public void getAllPolledData(RoutingContext context)
    {
        try
        {
            var query = new JsonObject()
                .put(QUERY, QueryConstant.GET_ALL_POLLED_DATA);

            executeQuery(query)
                .onComplete(queryResult ->
                {
                    if(queryResult.succeeded())
                    {
                        var result = queryResult.result();

                        APIUtils.sendSuccess(context,200,"Result of polling data" ,result);

                    }
                    else
                    {
                        var error = queryResult.cause();

                        APIUtils.sendError(context, 500, "Database query failed: " + error.getMessage());
                    }
                });
        }
        catch (Exception exception)
        {
            LOGGER.error("Error fetching polled data: {}", exception.getMessage());

            APIUtils.sendError(context, 500, "Internal server error");
        }
    }

}

