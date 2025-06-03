package com.example.NMS.cache;

import com.example.NMS.utility.DBUtils;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.example.NMS.constant.Constant.*;
import static com.example.NMS.constant.QueryConstant.GET_ACTIVE_METRIC_JOBS;

/**
 * In-memory cache for managing metric jobs in Lite NMS.
 * Stores metric job details (e.g., metric ID, provisioning job ID, IP, port, credentials) in a thread-safe
 * ConcurrentHashMap and handles initialization, updates, and polling intervals for metric collection.
 */
public class MetricCache implements cache
{

    private static MetricCache instance;

    private MetricCache(){}

    public static MetricCache getInstance()
    {
        if(instance == null)
        {
            instance =  new MetricCache();
        }

        return instance;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricCache.class);

    // Thread-safe cache of metric jobs: metric_id -> JsonObject
    public static final ConcurrentHashMap<Long, JsonObject> metricJobCache = new ConcurrentHashMap<>();

    // Flag to ensure cache is initialized only once
    private static boolean isCacheInitialized = false;

    /**
     * Initializes the cache by querying the database for enabled metric jobs.
     * Populates the cache with metric details, including IP, port, and credentials, and sets the initialization flag.
     * Skips initialization if the cache is already populated.
     */
    public void init()
    {
        if (isCacheInitialized)
        {
            LOGGER.info("Cache already initialized, skipping refresh");

            return;
        }

        // Execute the query and populate the cache
        DBUtils.executeQuery(new JsonObject().put(QUERY, GET_ACTIVE_METRIC_JOBS))
            .onComplete(queryResult ->
            {
                if(queryResult.succeeded())
                {
                    var result = queryResult.result();

                    result.forEach(entry ->
                    {
                        var metric = (JsonObject) entry;

                        var metricId = metric.getLong(METRIC_ID);

                        var job = new JsonObject()
                            .put(METRIC_ID, metricId)
                            .put(PROVISIONING_JOB_ID, metric.getLong(PROVISIONING_JOB_ID))
                            .put(METRIC_NAME, metric.getString(METRIC_NAME))
                            .put(IP, metric.getString(IP))
                            .put(PORT, metric.getInteger(PORT))
                            .put(PROTOCOL, metric.getString(PROTOCOL))
                            .put(CRED_DATA, metric.getJsonObject(CRED_DATA))
                            .put(ORIGINAL_INTERVAL, metric.getInteger(POLLING_INTERVAL))
                            .put(REMAINING_TIME, metric.getInteger(POLLING_INTERVAL))
                            .put(IS_ENABLED, metric.getBoolean(IS_ENABLED));


                        metricJobCache.put(metricId, job);
                    });

                    isCacheInitialized = true;

                    LOGGER.info("Initial cache populated with {} jobs", metricJobCache.size());
                }
                else
                {
                    var error = queryResult.cause();

                    LOGGER.error("Initial cache refresh failed: {}", error.getMessage());
                }
            });
    }

    public void insert(JsonObject job)
    {
        metricJobCache.put(job.getLong(METRIC_ID), job);

        LOGGER.info("Added metric job to cache: metric_id={}", job.getLong(METRIC_ID));
    }


    /**
     * Removes all metric jobs associated with a given provisioning job ID from the cache.
     *
     * @param provisioningJobId The provisioning job ID whose metric jobs should be removed.
     */
    public void delete(Long provisioningJobId)
    {
        var removedIds = metricJobCache.entrySet().stream()
            .filter(entry -> provisioningJobId.equals(entry.getValue().getLong(PROVISIONING_JOB_ID)))
            .map(Map.Entry::getKey)
            .toList();

        removedIds.forEach(metricJobCache::remove);

        if (!removedIds.isEmpty())
        {
            LOGGER.info("Removed {} metric jobs for provisioning_job_id={}", removedIds.size(), provisioningJobId);
        }
    }


    public void update(JsonObject job)
    {

        if (job.getBoolean(IS_ENABLED))
        {
            metricJobCache.put(job.getLong(METRIC_ID), job);
        }
        else
        {
            metricJobCache.remove(job.getLong(METRIC_ID));
        }


        LOGGER.info("Updated metric job in cache: metric_id={}", job.getLong(METRIC_ID));
    }


//    /**
//     * Handles the polling timer by decrementing remaining times for all metric jobs.
//     * Returns a list of jobs ready to be polled (remaining time <= 0), resetting their intervals.
//     *
//     * @return A list of metric job JSON objects ready for polling.
//     */
//    public static List<JsonObject> handleTimer()
//    {
//        var jobsToPoll = new ArrayList<JsonObject>();
//
//        // Decrement remaining time and collect jobs ready to poll
//        metricJobCache.forEach((metricId, job) ->
//        {
//            var newRemainingTime = job.getInteger(REMAINING_TIME) - TIMER_INTERVAL_SECONDS;
//
//            if (newRemainingTime <= 0)
//            {
//                jobsToPoll.add(job);
//
//                // Reset remaining time to original interval
//                job.put(REMAINING_TIME, job.getInteger(ORIGINAL_INTERVAL));
//            }
//            else
//            {
//                // Update remaining time
//                job.put(REMAINING_TIME, newRemainingTime);
//            }
//        });
//
//        if (!jobsToPoll.isEmpty())
//        {
//            LOGGER.info("Found {} jobs to poll", jobsToPoll.size());
//        }
//
//        return jobsToPoll;
//    }
}
