package com.example.NMS.polling;

import com.example.NMS.cache.MetricCache;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.example.NMS.constant.Constant.POLLING_BATCH_PROCESS;
import static com.example.NMS.constant.Constant.TIMER_INTERVAL_SECONDS;

public class Scheduler extends AbstractVerticle
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Scheduler.class);

    public void start(Promise<Void> startPromise)
    {
        try
        {
            MetricCache.init();

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

    private void handleScheduling(Long timerId)
    {
        var jobsToPoll = MetricCache.handleTimer();

        if (!jobsToPoll.isEmpty())
        {
            LOGGER.info("{} jobs to poll", jobsToPoll.size());

            vertx.eventBus().send(POLLING_BATCH_PROCESS, new JsonArray(jobsToPoll));
        }
    }

}
