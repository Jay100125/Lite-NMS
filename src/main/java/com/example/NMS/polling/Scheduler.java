package com.example.NMS.polling;

import com.example.NMS.cache.MetricCache;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.example.NMS.cache.MetricCache.metricJobCache;
import static com.example.NMS.constant.Constant.*;
import static com.example.NMS.constant.Constant.REMAINING_TIME;

/**
 * Vert.x verticle for scheduling metric polling in Lite NMS.
 * Initializes the metric cache and sets up a periodic timer to check for metric jobs ready to be polled.
 * Sends jobs to the event bus for batch processing when their polling intervals are reached.
 */
public class Scheduler extends AbstractVerticle
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Scheduler.class);

  /**
   * Starts the scheduler verticle.
   * Initializes the metric cache and sets up a periodic timer to trigger polling based on the configured interval.
   *
   * @param startPromise The promise to complete or fail based on startup success.
   */
    public void start(Promise<Void> startPromise)
    {
        try
        {
            // Initialize the metric cache
            MetricCache.getInstance().init();

            // Set up periodic timer for scheduling (convert seconds to milliseconds)
            vertx.setPeriodic(TIMER_INTERVAL_SECONDS * 1000, this::handleScheduling);

            LOGGER.info("Scheduler started with timer interval 10 seconds");

            startPromise.complete();
        }
        catch (Exception exception)
        {
            LOGGER.error("Failed to start Scheduler", exception);

            startPromise.fail(exception);
        }
    }

  /**
   * Handles periodic scheduling by checking for metric jobs ready to be polled.
   * Retrieves jobs from the metric cache and sends them to the event bus for batch processing.
   *
   * @param timerId The unique identifier of the timer event.
   */
    private void handleScheduling(Long timerId)
    {
        // Get metric jobs ready for polling
        var jobsToPoll = handleTimer();

        if (!jobsToPoll.isEmpty())
        {
            LOGGER.info("{} jobs to poll", jobsToPoll.size());

            // Send jobs to the event bus for batch processing
            vertx.eventBus().send(POLLING_BATCH_PROCESS, new JsonArray(jobsToPoll));
        }
    }

    /**
     * Handles the polling timer by decrementing remaining times for all metric jobs.
     * Returns a list of jobs ready to be polled (remaining time <= 0), resetting their intervals.
     *
     * @return A list of metric job JSON objects ready for polling.
     */
    public static List<JsonObject> handleTimer()
    {
        var jobsToPoll = new ArrayList<JsonObject>();

        // Decrement remaining time and collect jobs ready to poll
        metricJobCache.forEach((metricId, job) ->
        {
            var newRemainingTime = job.getInteger(REMAINING_TIME) - TIMER_INTERVAL_SECONDS;

            if (newRemainingTime <= 0)
            {
                jobsToPoll.add(job);

                // Reset remaining time to original interval
                job.put(REMAINING_TIME, job.getInteger(ORIGINAL_INTERVAL));
            }
            else
            {
                // Update remaining time
                job.put(REMAINING_TIME, newRemainingTime);
            }
        });

        if (!jobsToPoll.isEmpty())
        {
            LOGGER.info("Found {} jobs to poll", jobsToPoll.size());
        }

        return jobsToPoll;
    }

}
