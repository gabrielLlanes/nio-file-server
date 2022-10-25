package server;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;

public class RuntimeMetrics {

  private static Logger log = System.getLogger(RuntimeMetrics.class.getName());

  private static final Runtime runtime = Runtime.getRuntime();

  public static void logRuntimeMetrics() {
    log.log(Level.INFO, "{0}", runtimeMetricsString());
  }

  private static String runtimeMetricsString() {
    long totalMemory = runtime.totalMemory() / 1024;
    long freeMemory = runtime.freeMemory() / 1024;
    long memoryUsage = totalMemory - freeMemory;
    return String.format("Runtime %s:\n%d KB total memory, %d KB free memory.\n%d KB memory usage.\n",
        runtime.toString(), totalMemory, freeMemory, memoryUsage);
  }
}
