package com.example.NMS.plugin;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static com.example.NMS.constant.Constant.*;

/**
 * WORKER verticle that runs the Go plugin engine as a subprocess.
 * Writes the envelope to a temp file, spawns the binary, and drains stdout and
 * stderr concurrently (fixing the v1 pipe-buffer deadlock).
 */
public class Plugin extends AbstractVerticle {
    private static final Logger LOGGER = LoggerFactory.getLogger(Plugin.class);
    private static final String BINARY = "./plugin/Lite_NMS_Plugin";
    private static final long TIMEOUT_MINUTES = 2;

    @Override
    public void start(Promise<Void> startPromise) {
        vertx.eventBus().<JsonObject>localConsumer(PLUGIN_EXECUTE, message -> executePlugin(message.body()));
        LOGGER.info("PluginVerticle deployed");
        startPromise.complete();
    }

    private void executePlugin(JsonObject envelope) {
        Path envFile = null;
        Process process = null;
        Thread stderrThread = null;
        Thread watchdog = null;
        try {
            String encoded = Base64.getEncoder().encodeToString(
                envelope.encode().getBytes(StandardCharsets.UTF_8));
            envFile = Files.createTempFile("nms-env-", ".b64");
            Files.writeString(envFile, encoded);

            process = new ProcessBuilder(BINARY, envFile.toString()).start();
            final Process p = process;

            // Drain stderr on a separate thread so a full stderr pipe cannot deadlock stdout.
            stderrThread = new Thread(() -> drainStderr(p));
            stderrThread.setDaemon(true);
            stderrThread.start();

            // Watchdog enforces a hard wall-clock timeout even if the engine hangs without
            // closing stdout: it force-destroys the process, which EOFs stdout and unblocks
            // the read loop below.
            watchdog = new Thread(() -> {
                try {
                    if (!p.waitFor(TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
                        LOGGER.warn("Plugin engine timed out after {} min; destroying", TIMEOUT_MINUTES);
                        p.destroyForcibly();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            watchdog.setDaemon(true);
            watchdog.start();

            try (BufferedReader stdout = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = stdout.readLine()) != null) {
                    JsonObject result = decodeResultLine(line.trim());
                    if (result != null) {
                        result.put("timestamp", System.currentTimeMillis());
                        vertx.eventBus().send(STORAGE_RESULTS, result);
                    }
                }
            }

            int exitCode = process.waitFor(); // returns promptly: normal exit or watchdog kill
            watchdog.interrupt();
            stderrThread.join(1000);
            if (exitCode != 0) {
                LOGGER.warn("Plugin engine exited with code {}", exitCode);
            }
        } catch (Exception e) {
            LOGGER.error("Error running plugin engine: {}", e.getMessage());
        } finally {
            if (watchdog != null) watchdog.interrupt();
            if (process != null && process.isAlive()) process.destroyForcibly();
            if (envFile != null) {
                try { Files.deleteIfExists(envFile); } catch (Exception ignore) {}
            }
            vertx.eventBus().send(EVENT_COMPLETION, envelope);
        }
    }

    private void drainStderr(Process process) {
        try (BufferedReader err = new BufferedReader(
                new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = err.readLine()) != null) {
                LOGGER.debug("[engine] {}", line);
            }
        } catch (Exception ignore) {}
    }

    /** Decodes one base64(JSON) result line; returns null on malformed input. */
    public static JsonObject decodeResultLine(String base64) {
        try {
            byte[] decoded = Base64.getDecoder().decode(base64);
            return new JsonObject(new String(decoded, StandardCharsets.UTF_8));
        } catch (Exception e) {
            return null;
        }
    }
}
