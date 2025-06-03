package com.example.NMS.utility;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Utility class for network-related operations in Lite NMS.
 * Provides methods for validating IPv4 addresses, resolving IP ranges and CIDR notations,
 * checking IP reachability and port availability, and executing SSH plugins for network discovery.
 */
public class Utility
{
    public static final Logger LOGGER = LoggerFactory.getLogger(Utility.class.getName());

    // Regular expression for validating IPv4 addresses
    private static final String IPv4_PATTERN = "^((25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d)(\\.|$)){4}$";

  /**
   * Validates whether the given string is a valid IPv4 address.
   *
   * @param ip The IP address to validate.
   * @return True if the IP address is valid, false otherwise.
   */
    public static boolean isValidIPv4(String ip)
    {
      return ip != null && ip.matches(IPv4_PATTERN);
    }

  /**
   * Resolves IP addresses from a given input string.
   * Supports single IP addresses, IP ranges (e.g., "10.20.40.10 - 10.20.40.123"),
   * and CIDR notations (e.g., "10.20.40.0/16").
   *
   * @param ipInput The input string containing a single IP, IP range, or CIDR.
   * @return A list of resolved IP addresses.
   * @throws Exception If the input format is invalid or resolution fails.
   */
    public static List<String> resolveIpAddresses(String ipInput) throws Exception
    {
        var ipList = new ArrayList<String>();

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

  /**
   * Converts an IP address to a long integer for range calculations.
   *
   * @param ip The InetAddress object representing the IP address.
   * @return The long integer representation of the IP address.
   */
    private static long ipToLong(InetAddress ip)
    {
        byte[] octets = ip.getAddress();

        var result = 0;

        for (byte octet : octets)
        {
            result <<= 8;

            result |= (octet & 0xff);
        }
        return result;
    }

  /**
   * Converts a long integer to an IP address string.
   *
   * @param ip The long integer representing the IP address.
   * @return The IP address as a string (e.g., "192.168.1.1").
   */
    private static String longToIp(long ip)
    {
        return String.format("%d.%d.%d.%d",
          (ip >> 24) & 0xff,
          (ip >> 16) & 0xff,
          (ip >> 8) & 0xff,
          ip & 0xff);
    }



  /**
   * Checks the reachability of a list of IP addresses and verifies if a specified port is open.
   * Uses `fping` to check IP reachability and `nc` to check port availability.
   *
   * @param ipAddresses The list of IP addresses to check.
   * @param port        The port to verify for each IP address.
   * @return A JSON array of results, each containing the IP, reachability status, and port status.
   * @throws Exception If the reachability check fails due to invalid input or system errors.
   */
    public static JsonArray checkReachability(List<String> ipAddresses, int port) throws Exception
    {
        var results = new JsonArray();

        var aliveIps = new HashSet<String>();

        // Run bulk fping with -a to get alive hosts
        try
        {
            var command = new ArrayList<String>();

            command.add("fping");
            command.add("-a"); // Show alive hosts
            command.add("-q"); // Quiet mode
            command.add("-c"); // Count
            command.add("1");  // 1 attempt
            command.add("-t"); // Timeout
            command.add("1000"); // 1000ms
            command.addAll(ipAddresses); // Add all IPs

            var processBuilder = new ProcessBuilder(command);

            var process = processBuilder.start();

            LOGGER.info("Ip {} fping command: {}", ipAddresses, String.join(" ", command));

            // Read alive IPs from stdout (fping -a outputs alive hosts directly)
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

            LOGGER.info("fping alive IPs: {}", aliveIps);

            // Log stderr for debugging
            var stderrReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));

            var stderr = new StringBuilder();

            while ((line = stderrReader.readLine()) != null)
            {
                stderr.append(line).append("\n");
            }

            if (!stderr.isEmpty())
            {
                LOGGER.debug("fping stderr: {}", stderr);
            }

            var exitCode = process.waitFor();

            if (exitCode != 0 && aliveIps.isEmpty())
            {
                LOGGER.warn("fping exited with code {} and no alive IPs", exitCode);
            }
        }
        catch (Exception exception)
        {
            LOGGER.error("Error running fping: {}", exception.getMessage());
        }

        // Check port for each IP
        for (var ip : ipAddresses)
        {
            var isReachable = aliveIps.contains(ip);

            var isPortOpen = false;

            if (isReachable)
            {
                try
                {
                    // Use nc to check port
                    isPortOpen = isPortOpen(ip, port);

                    LOGGER.debug("Port check for IP {} on port {}: {}", ip, port, isPortOpen ? "open" : "closed");
                }
                catch (Exception exception)
                {
                    LOGGER.error("Error checking port {} for IP {}: {}", port, ip, exception.getMessage());

                    isPortOpen = false;
                }
            }
            else
            {
                LOGGER.debug("IP {} is not reachable, skipping port check", ip);
            }
            results.add(new JsonObject()
              .put("ip", ip)
              .put("reachable", isReachable)
              .put("port_open", isPortOpen));
        }

        LOGGER.info("Reachability results: {}", results.encode());

        return results;
    }

    private static boolean isPortOpen(String ip, int port)
    {
        try (var socket = new Socket())
        {
            // Set connection timeout to 1 second (1000ms) to match nc -w 1 behavior
            socket.connect(new InetSocketAddress(ip, port), 1000);

            return true;
        }
        catch (Exception exception)
        {
            // Connection failed - port is closed or unreachable
            return false;
        }
    }
}
