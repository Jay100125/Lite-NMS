package com.example.NMS;

import com.example.NMS.api.Server;
import com.example.NMS.database.Database;
import com.example.NMS.discovery.Discovery;
import com.example.NMS.plugin.Plugin;
import com.example.NMS.plugin.ResponseProcessor;
import com.example.NMS.polling.Polling;
import com.example.NMS.polling.Scheduler;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.ThreadingModel;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static com.example.NMS.constant.Constant.MAX_WORKER_EXECUTION_TIME;

public class Main
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static final Vertx vertx = Vertx.vertx(new VertxOptions().setMaxWorkerExecuteTime(MAX_WORKER_EXECUTION_TIME).setMaxWorkerExecuteTimeUnit(TimeUnit.SECONDS));

    public static void main(String[] args)
    {
        LOGGER.info("Starting NMS");

        vertx.deployVerticle(new Server())

            .compose(response -> vertx.deployVerticle(Database.class.getName()))

            .compose(response -> vertx.deployVerticle(Discovery.class.getName()))

//            .compose(response -> vertx.deployVerticle(Scheduler.class.getName()))
//
//            .compose(response -> vertx.deployVerticle(Polling.class.getName()))

            .compose(response -> vertx.deployVerticle(Plugin.class.getName(),  new DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)))

            .compose(response -> vertx.deployVerticle(ResponseProcessor.class.getName()))

            .onComplete(handler -> {

                if (handler.succeeded())
                {
                    LOGGER.info("Application started");
                }
                else
                {
                    LOGGER.error("Application failed to start {}", handler.cause().getMessage());

                    vertx.close(shutdown -> {

                        if (shutdown.succeeded())
                        {
                            LOGGER.info("Vert.x instance shut down successfully");
                        }
                        else
                        {
                            LOGGER.error("Failed to shut down Vert.x instance: {}", shutdown.cause().getMessage());
                        }
                    });
                }
           });
    }

}
