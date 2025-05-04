package com.example.NMS.utility;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Utility
{
    public static final Logger logger = LoggerFactory.getLogger(Utility.class.getName());

    private static final String IPv4_PATTERN = "^((25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d)(\\.|$)){4}$";

    public static boolean isValidIPv4(String ip)
    {
      return ip != null && ip.matches(IPv4_PATTERN);
    }

  /**
   * Resolve IP addresses from a given input string.
   * @param ipInput
   * @return
   * @throws Exception
   */
    public static List<String> resolveIpAddresses(String ipInput) throws Exception
    {
      List<String> ipList = new ArrayList<>();

      if (ipInput.contains("-"))
      {
        // Handle IP range (e.g., "10.20.40.10 - 10.20.40.123")
        String[] range = ipInput.split("\\s*-\\s*");

        if (range.length != 2)
        {
          throw new Exception("Invalid IP range format");
        }

        var startIp = range[0].trim();

        var endIp = range[1].trim();

        var start = ipToLong(InetAddress.getByName(startIp));

        var end = ipToLong(InetAddress.getByName(endIp));

        if (start > end)
        {
          throw new Exception("Start IP must be less than or equal to end IP");
        }

        for (var i = start; i <= end; i++)
        {
          ipList.add(longToIp(i));
        }
      }
      else if (ipInput.contains("/"))
      {
        // Handle CIDR (e.g., "10.20.40.0/16")
        String[] cidrParts = ipInput.split("/");

        if (cidrParts.length != 2)
        {
          throw new IllegalArgumentException("Invalid CIDR format");
        }

        var baseIp = cidrParts[0].trim();

        int maskBits;
        try
        {
          maskBits = Integer.parseInt(cidrParts[1].trim());
        }
        catch (Exception e)
        {
          throw new IllegalArgumentException("Invalid CIDR mask");
        }

        var base = ipToLong(InetAddress.getByName(baseIp));

        var mask = (0xffffffffL << (32 - maskBits));

        var start = base & mask;

        var end = start + (1L << (32 - maskBits)) - 1;

        for (var i = start; i <= end; i++)
        {
          ipList.add(longToIp(i));
        }
      }
      else
      {
        // Handle single IP
        if (!Utility.isValidIPv4(ipInput))
        {
          throw new IllegalArgumentException("Invalid IP address");
        }
        ipList.add(ipInput);
      }

      return ipList;

    }

    private static long ipToLong(InetAddress ip)
    {
      byte[] octets = ip.getAddress();

      long result = 0;

      for (byte octet : octets)
      {
        result <<= 8;

        result |= (octet & 0xff);
      }
      return result;
    }

    private static String longToIp(long ip)
    {
      return String.format("%d.%d.%d.%d",
        (ip >> 24) & 0xff,
        (ip >> 16) & 0xff,
        (ip >> 8) & 0xff,
        ip & 0xff);
    }

    public static JsonArray checkReachability(List<String> ipList, int port) throws Exception
    {
      var results = new JsonArray();

      Set<String> aliveIps = new HashSet<>();

      // Run bulk fping with -a to get alive hosts
      try
      {
        List<String> command = new ArrayList<>();

        command.add("fping");
        command.add("-a"); // Show alive hosts
        command.add("-q"); // Quiet mode
        command.add("-c"); // Count
        command.add("1");  // 1 attempt
        command.add("-t"); // Timeout
        command.add("1000"); // 1000ms
        command.addAll(ipList); // Add all IPs

        var pb = new ProcessBuilder(command);

        var process = pb.start();

        logger.info("Ip {} fping command: {}", ipList, String.join(" ", command));
        // Read alive IPs from stdout
        var reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));

        String line;

        while ((line = reader.readLine()) != null)
        {
          // Example line: "192.168.1.1 : xmt/rcv/%loss = 3/3/0%, min/avg/max = 1.01/1.23/1.45"
          if (!line.contains("100%"))
          {
            aliveIps.add(line.split(":")[0].trim());
          }
        }


        for (var ip : aliveIps)
        {
          logger.info("fping alive IP: {}", ip);

        }

        logger.info("FPING command: {}", String.join(" ", command));
        // Log stderr for debugging

        BufferedReader stderrReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));

        StringBuilder stderr = new StringBuilder();

        while ((line = stderrReader.readLine()) != null)
        {
          stderr.append(line).append("\n");
        }

        if (!stderr.isEmpty())
        {
          logger.debug("fping stderr: {}", stderr);
        }

        var exitCode = process.waitFor();

        if (exitCode != 0 && aliveIps.isEmpty())
        {
          logger.warn("fping exited with code {} and no alive IPs", exitCode);
        }
      }
      catch (Exception e)
      {
        logger.error("Error running fping: {}", e.getMessage());

//        throw e; // Let caller handle
      }

      // Check port for each IP
      for (var ip : ipList)
      {
        boolean isReachable = aliveIps.contains(ip);

        boolean isPortOpen = false;

        if (isReachable)
        {
          try
          {
            // Use nc to check port
            ProcessBuilder pb = new ProcessBuilder("nc", "-z", "-w", "1", ip, String.valueOf(port));

            Process process = pb.start();

            int exitCode = process.waitFor();

            isPortOpen = exitCode == 0;

            logger.debug("Port check for IP {} on port {}: {}", ip, port, isPortOpen ? "open" : "closed");
          }
          catch (Exception e)
          {
            logger.error("Error checking port {} for IP {}: {}", port, ip, e.getMessage());

            isPortOpen = false;
          }
        }
        else
        {
          logger.debug("IP {} is not reachable, skipping port check", ip);
        }
        results.add(new JsonObject()
          .put("ip", ip)
          .put("reachable", isReachable)
          .put("port_open", isPortOpen));
      }

      logger.info("Reachability results: {}", results.encode());

      return results;
    }

    public static JsonArray runSSHPlugin(JsonObject pluginJson)
    {
      JsonArray results = new JsonArray();

      Process process = null;

      BufferedReader stdInput = null;

      BufferedReader stdError = null;

      OutputStreamWriter stdOutput = null;

      logger.info("-----------------------------------------------------------");
      try
      {
        // Start the SSH plugin process
        ProcessBuilder pb = new ProcessBuilder("./plugin/ssh_plugin");

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

            String decoded = new String(decodedBytes, StandardCharsets.UTF_8);

            results.add(new JsonObject(decoded));
          }
          catch (Exception e)
          {
            logger.error("Failed to decode stdout line '{}': {}", line, e.getMessage());
          }
        }

        // Read stderr for debugging
        stdError = new BufferedReader(new InputStreamReader(process.getErrorStream()));

        StringBuilder stderr = new StringBuilder();

        while ((line = stdError.readLine()) != null)
        {
          stderr.append(line).append("\n");
        }
  //      if (stderr.length() > 0) {
  //        logger.warn("SSH plugin stderr: {}", stderr.toString());
  //      }

        // Wait for the process to exit
        int exitCode = process.waitFor();

        if (exitCode != 0)
        {
          logger.warn("SSH plugin exited with code {}", exitCode);

          if (results.isEmpty())
          {
            results.add(new JsonObject()
              .put("status", "failed")
              .put("error", "SSH plugin failed with exit code: " + exitCode));
          }
        }

        // If no results were received, add an error
        if (results.isEmpty())
        {
          results.add(new JsonObject()
            .put("status", "failed")
            .put("error", "No data returned from SSH plugin"));
        }

      }
      catch (Exception e)
      {
        logger.error("Error running SSH plugin: {}", e.getMessage());

        results.add(new JsonObject()
          .put("status", "failed")
          .put("error", "Failed to run SSH plugin: " + e.getMessage()));
      }
      finally
      {
        try
        {
          if (stdInput != null)
          {
            stdInput.close();
          }
          if (stdError != null)
          {
            stdError.close();
          }
          if (stdOutput != null)
          {
            stdOutput.close();
          }
          if (process != null && process.isAlive())
          {
            process.destroy();

            process.waitFor(6, java.util.concurrent.TimeUnit.SECONDS);
          }
        }
        catch (Exception e)
        {
          logger.error("Error cleaning up SSH plugin process: {}", e.getMessage());
        }
      }
      return results;
    }
}
