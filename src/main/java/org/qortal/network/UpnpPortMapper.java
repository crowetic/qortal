package org.qortal.network;

import com.dosse.upnp.UPnP;
import com.google.common.net.InetAddresses;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/** Serializes access to WaifUPnP's process-wide gateway and coordinates both listeners' mappings. */
final class UpnpPortMapper {
    private static final Logger LOGGER = LogManager.getLogger(UpnpPortMapper.class);
    private static final long SHUTDOWN_WAIT_SECONDS = 5;

    private final ExecutorService worker = Executors.newSingleThreadExecutor(task -> {
        Thread thread = new Thread(task, "UPnP");
        thread.setDaemon(true);
        return thread;
    });
    private final Gateway gateway;
    private final Map<Integer, Mapping> mappings = new LinkedHashMap<>();
    private volatile boolean stopping;

    UpnpPortMapper() {
        this(new Gateway());
    }

    UpnpPortMapper(Gateway gateway) {
        this.gateway = gateway;
    }

    static class Gateway {
        boolean available() { return UPnP.isUPnPAvailable(); }
        boolean isMapped(int port) { return UPnP.isMappedTCP(port); }
        boolean open(int port) { return UPnP.openPortTCP(port); }
        boolean close(int port) { return UPnP.closePortTCP(port); }

        String mappedAddress(int port) {
            return UPnP.isMappedTCP(port) ? UPnP.getExternalAddress() : null;
        }
    }

    private static final class Mapping {
        Future<?> setup;
        volatile boolean cancelled;
        // Only the serial worker reads or writes this field.
        boolean removeOnShutdown;
    }

    synchronized void configure(int port, boolean enabled, Consumer<String> publishAddress) {
        if (stopping || mappings.containsKey(port))
            return;

        Mapping mapping = new Mapping();
        mappings.put(port, mapping);
        mapping.setup = worker.submit(() -> {
            try {
                // Discover before any mutation. A late discovery must never open a port after shutdown.
                if (enabled && stopping)
                    return;
                if (!gateway.available()) {
                    if (enabled && !stopping)
                        LOGGER.warn("No UPnP gateway found for TCP port {}", port);
                    return;
                }
                if (mapping.cancelled || (enabled && stopping))
                    return;

                if (!enabled) {
                    // Preserve the configured removal of a stale mapping.
                    if (!gateway.close(port))
                        LOGGER.debug("No UPnP mapping removed for TCP port {}", port);
                    return;
                }

                // If the add reply is lost, never remove a mapping that already existed before our request.
                boolean alreadyMapped = gateway.isMapped(port);
                if (stopping)
                    return;
                boolean opened = gateway.open(port);
                // A cancelled HTTP request can have reached the gateway before its reply was interrupted.
                mapping.removeOnShutdown = opened || (mapping.cancelled && !alreadyMapped);
                if (stopping)
                    return;
                if (!opened) {
                    LOGGER.warn("Unable to map TCP port {} with UPnP", port);
                    return;
                }

                String address = gateway.mappedAddress(port);
                if (address == null || !InetAddresses.isInetAddress(address) || InetAddresses.forString(address).isAnyLocalAddress()) {
                    LOGGER.warn("Could not confirm the external address for UPnP port {}", port);
                    return;
                }
                if (!stopping) {
                    publishAddress.accept(address);
                    LOGGER.info("UPnP mapped TCP port {}", port);
                }
            } catch (RuntimeException e) {
                LOGGER.warn("UPnP setup failed for TCP port {}", port, e);
            }
        });
    }

    /** Finish router work while the listeners still own their ports, before a replacement core can bind. */
    synchronized void shutdown() {
        if (stopping)
            return;
        stopping = true;
        boolean interrupted = Thread.interrupted();
        try {
            for (Mapping mapping : mappings.values()) {
                try {
                    mapping.setup.get(SHUTDOWN_WAIT_SECONDS, TimeUnit.SECONDS);
                } catch (TimeoutException | InterruptedException e) {
                    interrupted |= e instanceof InterruptedException;
                    mapping.cancelled = true;
                    mapping.setup.cancel(true);
                } catch (ExecutionException e) {
                    LOGGER.warn("UPnP worker failed", e.getCause());
                }
            }

            for (Map.Entry<Integer, Mapping> entry : mappings.entrySet()) {
                int port = entry.getKey();
                Future<?> cleanup = worker.submit(() -> {
                    if (entry.getValue().removeOnShutdown && !gateway.close(port))
                        LOGGER.warn("Unable to remove UPnP mapping for TCP port {}", port);
                });
                try {
                    cleanup.get(SHUTDOWN_WAIT_SECONDS, TimeUnit.SECONDS);
                } catch (TimeoutException | InterruptedException e) {
                    interrupted |= e instanceof InterruptedException;
                    cleanup.cancel(true);
                    LOGGER.warn("Could not confirm UPnP cleanup for TCP port {}", port);
                } catch (ExecutionException e) {
                    LOGGER.warn("UPnP cleanup failed for TCP port {}", port, e.getCause());
                }
            }
        } finally {
            worker.shutdownNow();
            try {
                if (!worker.awaitTermination(SHUTDOWN_WAIT_SECONDS, TimeUnit.SECONDS))
                    LOGGER.warn("UPnP worker did not terminate before network shutdown");
            } catch (InterruptedException e) {
                interrupted = true;
            }
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }
}
