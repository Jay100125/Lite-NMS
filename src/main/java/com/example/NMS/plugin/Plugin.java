package com.example.NMS.plugin;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static com.example.NMS.constant.Constant.*;

/**
 * Vert.x verticle for executing the Lite NMS SSH plugin.
 * Listens for plugin execution requests on the event bus, runs the plugin process,
 * and forwards results to the responseProcessor for database storage.
 */
public class Plugin extends AbstractVerticle
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Plugin.class);

    @Override
    public void start(Promise<Void> startPromise)
    {
        // Set up event bus consumer for plugin execution requests
        vertx.eventBus().<JsonObject>localConsumer(PLUGIN_EXECUTE, message ->
        {
            var pluginJson = message.body();

            LOGGER.info("Received plugin execution request: {}", pluginJson.encodePrettily());

            executePlugin(pluginJson);
        });

        LOGGER.info("PluginVerticle deployed");

        startPromise.complete();
    }

    /**
     * Executes the SSH plugin with the provided JSON configuration.
     * Sends Base64-encoded JSON input to the plugin via stdin, reads Base64-encoded JSON results from stdout,
     * and forwards the results to the StorageVerticle.
     *
     * @param pluginJson The JSON object containing the plugin configuration.
     */
    private void executePlugin(JsonObject pluginJson)
    {
        var results = new JsonArray();

        Process process = null;

        BufferedReader stdInput = null;

        BufferedReader stdError = null;

        OutputStreamWriter stdOutput = null;

        try
        {
            // Start the SSH plugin process
            var pb = new ProcessBuilder("./plugin/Lite_NMS_Plugin");

            process = pb.start();

            // Write Base64-encoded JSON to stdin
            stdOutput = new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8);

            String encodedInput = Base64.getEncoder().encodeToString(pluginJson.encode().getBytes(StandardCharsets.UTF_8));

            stdOutput.write(encodedInput + "\n");

            stdOutput.flush();

            stdOutput.close();

            // Read Base64-encoded JSON results from stdout
            stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));

            String line;

            while ((line = stdInput.readLine()) != null)
            {
                try
                {
                    byte[] decodedBytes = Base64.getDecoder().decode(line.trim());

                    var decoded = new String(decodedBytes, StandardCharsets.UTF_8);

                    var resultObj = new JsonObject(decoded);

                    resultObj.put("timestamp", System.currentTimeMillis());

                    vertx.eventBus().send(STORAGE_RESULTS, resultObj);
                }
                catch (Exception exception)
                {
                    LOGGER.error("Failed to decode stdout line '{}': {}", line, exception.getMessage());
                }
            }

            // Read stderr for debugging
            stdError = new BufferedReader(new InputStreamReader(process.getErrorStream()));

            var stderr = new StringBuilder();

            while ((line = stdError.readLine()) != null)
            {
                stderr.append(line).append("\n");
            }

            var exitCode = process.waitFor(2, TimeUnit.MINUTES) ? process.exitValue() : -1;

            if (exitCode != 0)
            {
                LOGGER.warn("SSH plugin exited with code {}", exitCode);

                if (results.isEmpty())
                {
                    results.add(new JsonObject()
                        .put(STATUS, "failed")
                        .put(ERROR, "SSH plugin failed with exit code: " + exitCode));
                }
            }
        }
        catch (Exception exception)
        {
            LOGGER.error("Error running SSH plugin: {}", exception.getMessage());

            results.add(new JsonObject()
                .put(STATUS, "failed")
                .put(ERROR, "Failed to run SSH plugin: " + exception.getMessage()));
        }
        finally
        {
            try
            {
                if (stdInput != null) stdInput.close();

                if (stdError != null) stdError.close();

                if (stdOutput != null) stdOutput.close();

                if (process != null && process.isAlive()) process.destroyForcibly();
            }
            catch (Exception exception)
            {
                LOGGER.error("Error cleaning up SSH plugin process: {}", exception.getMessage());
            }

            vertx.eventBus().send(EVENT_COMPLETION, pluginJson);
        }
    }
}

