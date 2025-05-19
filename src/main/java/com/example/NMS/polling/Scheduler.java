package com.example.NMS.polling;

import com.example.NMS.cache.MetricCache;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.example.NMS.constant.Constant.POLLING_BATCH_PROCESS;
import static com.example.NMS.constant.Constant.TIMER_INTERVAL_SECONDS;

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
            MetricCache.init();

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
        var jobsToPoll = MetricCache.handleTimer();

        if (!jobsToPoll.isEmpty())
        {
            LOGGER.info("{} jobs to poll", jobsToPoll.size());

            // Send jobs to the event bus for batch processing
            vertx.eventBus().send(POLLING_BATCH_PROCESS, new JsonArray(jobsToPoll));
        }
    }

}
