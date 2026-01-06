package org.qortal.controller;

import com.rust.litewalletjni.LiteWalletJni;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.qortal.arbitrary.ArbitraryDataFile;
import org.qortal.arbitrary.ArbitraryDataReader;
import org.qortal.arbitrary.ArbitraryDataResource;
import org.qortal.arbitrary.exception.MissingDataException;
import org.qortal.crosschain.ForeignBlockchainException;
import org.qortal.crosschain.PirateChain;
import org.qortal.crosschain.PirateLightClient;
import org.qortal.crosschain.PirateWallet;
import org.qortal.crosschain.ChainableServer;
import org.qortal.data.arbitrary.ArbitraryResourceStatus;
import org.qortal.data.transaction.ArbitraryTransactionData;
import org.qortal.data.transaction.TransactionData;
import org.qortal.network.Network;
import org.qortal.network.Peer;
import org.qortal.repository.DataException;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.settings.Settings;
import org.qortal.transaction.ArbitraryTransaction;
import org.qortal.utils.ArbitraryTransactionUtils;
import org.qortal.utils.Base58;
import org.qortal.utils.FilesystemUtils;
import org.qortal.utils.NTP;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class PirateChainWalletController extends Thread {

    protected static final Logger LOGGER = LogManager.getLogger(PirateChainWalletController.class);

    private static PirateChainWalletController instance;

    final private static long SAVE_INTERVAL = 60 * 60 * 1000L; // 1 hour
    private long lastSaveTime = 0L;

    private boolean running;
    private volatile PirateWallet currentWallet = null;
    private volatile boolean shouldLoadWallet = false;
    private volatile String loadStatus = null;

    private void updateLoadStatus(String status) {
        this.loadStatus = status;
        if (status != null) {
            LOGGER.info(status);
        }
    }

    private static final long WALLET_LOCK_TIMEOUT_MS = 5_000L;
    private static final long SWITCH_LOCK_TIMEOUT_MS = 30_000L;
    private static final long SYNC_STOP_TIMEOUT_MS = 30_000L;
    private static final long SYNC_WAIT_INTERVAL_MS = 250L;
    private static final long STATUS_LOCK_TIMEOUT_MS = 500L;
    private static final long SKIP_LOG_INTERVAL_MS = 30_000L;
    private static final long SYNC_RUNNING_LOG_INTERVAL_MS = 30_000L;
    private final Object statusLock = new Object();

    private static final String NULL_SEED_ENTROPY58 = Base58.encode(new byte[32]);

    private final ReentrantLock walletLock = new ReentrantLock(true);
    private final Object syncMonitor = new Object();
    private final Object switchingMonitor = new Object();
    private volatile boolean syncInProgress = false;
    private volatile long syncStartTimeMs = 0L;
    private volatile long lastSyncRunningLogTime = 0L;
    private static final long WALLET_INIT_RETRY_DELAY_MS = 100_000L;
    private static final int WALLET_INIT_MAX_RETRIES = 5;
    private static final long SYNC_CALL_WARN_MS = 120_000L;
    private static final long SYNC_FORCE_STOP_MS = 5 * 60 * 1000L;
    private static final long SYNC_WATCHDOG_INTERVAL_MS = 30_000L;

    private volatile boolean walletReconnectRequested = false;
    private volatile long lastWalletInitFailureMs = 0L;
    private volatile byte[] lastEntropyBytes;
    private volatile boolean lastIsNullSeedWallet = false;
    private volatile Thread switchingThread = null;
    private int switchingDepth = 0;
    private final ThreadLocal<Integer> switchingClaims = ThreadLocal.withInitial(() -> 0);
    private volatile long lastSkipLogTime = 0L;
    private volatile String lastSkipReason = null;
    private int walletInitRetryCount = 0;
    private final Semaphore syncStatusSemaphore = new Semaphore(1);
    private final ScheduledExecutorService syncWatchdogExecutor = Executors.newSingleThreadScheduledExecutor();
    private final AtomicBoolean syncWatchdogStarted = new AtomicBoolean(false);
    private final AtomicBoolean forceSaveAfterSync = new AtomicBoolean(false);
    private final AtomicBoolean syncStopRequested = new AtomicBoolean(false);
    private final AtomicBoolean saveRequested = new AtomicBoolean(false);
    private volatile Thread controllerThread = null;

    private static String qdnWalletSignature = "4DtYWqBSsPaeY8u42zpWQuxogN1N9USbYFuidgaXfxNv5gneNtkVXSd7Lani7dGq7WpTZZzPfBcBhG349FXbQiUn";

    private PirateChainWalletController() {
        this.running = true;
    }

    public static PirateChainWalletController getInstance() {
        if (instance == null)
            instance = new PirateChainWalletController();

        return instance;
    }

    private boolean isSwitching() {
        synchronized (this.switchingMonitor) {
            return this.switchingThread != null;
        }
    }

    private boolean isSwitchingByCurrentThread() {
        synchronized (this.switchingMonitor) {
            return Thread.currentThread() == this.switchingThread;
        }
    }

    private boolean claimSwitching() {
        synchronized (this.switchingMonitor) {
            if (this.switchingThread == null) {
                this.switchingThread = Thread.currentThread();
                this.switchingDepth = 1;
            } else if (this.switchingThread == Thread.currentThread()) {
                this.switchingDepth++;
            } else {
                return false;
            }
        }
        this.switchingClaims.set(this.switchingClaims.get() + 1);
        return true;
    }

    private void releaseSwitchingClaim() {
        int claims = this.switchingClaims.get();
        if (claims > 0) {
            this.switchingClaims.set(claims - 1);
            synchronized (this.switchingMonitor) {
                if (this.switchingThread == Thread.currentThread()) {
                    this.switchingDepth--;
                    if (this.switchingDepth <= 0) {
                        this.switchingDepth = 0;
                        this.switchingThread = null;
                    }
                }
            }
        }
    }

    private boolean acquireWalletLock(long timeoutMs) {
        try {
            return this.walletLock.tryLock(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void releaseWalletLockIfHeld() {
        if (this.walletLock.isHeldByCurrentThread()) {
            this.walletLock.unlock();
        }
    }

    private boolean waitForSyncIdle(long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        synchronized (this.syncMonitor) {
            while (this.syncInProgress) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    return false;
                }
                long waitTime = Math.min(remaining, SYNC_WAIT_INTERVAL_MS);
                try {
                    this.syncMonitor.wait(waitTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        return true;
    }

    private void logSyncSkip(String reason) {
        long now = System.currentTimeMillis();
        if (!Objects.equals(reason, this.lastSkipReason) || now - this.lastSkipLogTime >= SKIP_LOG_INTERVAL_MS) {
            LOGGER.info("Pirate wallet sync skipped: {}", reason);
            this.lastSkipReason = reason;
            this.lastSkipLogTime = now;
        }
    }

    private void logSyncRunningIfNeeded() {
        if (!this.syncInProgress || this.syncStartTimeMs <= 0L) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now - this.lastSyncRunningLogTime >= SYNC_RUNNING_LOG_INTERVAL_MS) {
            long elapsedMs = now - this.syncStartTimeMs;
            LOGGER.info("Pirate wallet sync still running ({} ms elapsed)", elapsedMs);
            this.lastSyncRunningLogTime = now;
        }
    }

    private String formatSwitchingStatus() {
        if (this.loadStatus != null) {
            return "Wallet switch in progress (" + this.loadStatus + ")";
        }
        return "Wallet switch in progress";
    }

    private void stopSyncIfRunning(long timeoutMs) {
        if (!this.syncInProgress) {
            return;
        }

        try {
            LiteWalletJni.execute("stop", "");
        } catch (RuntimeException e) {
            LOGGER.debug("Unable to stop Pirate Chain sync: {}", e.getMessage());
        }

        if (!this.waitForSyncIdle(timeoutMs)) {
            LOGGER.info("Timed out waiting for Pirate Chain sync to stop");
        }
    }

    private void rotateLightwalletServer(String reason) {
        PirateChain pirateChain = PirateChain.getInstance();
        if (!(pirateChain.getBlockchainProvider() instanceof PirateLightClient)) {
            return;
        }

        PirateLightClient lightClient = (PirateLightClient) pirateChain.getBlockchainProvider();
        List<ChainableServer> candidates = new ArrayList<>(lightClient.getServers());
        ChainableServer currentServer = lightClient.getCurrentServer();
        if (currentServer != null) {
            candidates.remove(currentServer);
        }
        candidates.removeAll(lightClient.getUselessServers());

        if (candidates.isEmpty()) {
            LOGGER.info("No alternate Pirate lightwallet servers available to rotate");
            return;
        }

        ChainableServer next = candidates.get(new Random().nextInt(candidates.size()));
        try {
            LOGGER.info("Switching Pirate lightwallet server to {} ({})", next, reason);
            lightClient.setCurrentServer(next, "PirateChainWalletController");
            if (this.currentWallet != null) {
                this.walletReconnectRequested = true;
                LOGGER.info("Pirate wallet reconnect requested after server switch");
            }
        } catch (ForeignBlockchainException e) {
            LOGGER.info("Unable to switch Pirate lightwallet server: {}", e.getMessage());
        }
    }

    private String getLightwalletStatusSummary() {
        PirateChain pirateChain = PirateChain.getInstance();
        if (pirateChain.getBlockchainProvider() instanceof PirateLightClient) {
            PirateLightClient lightClient = (PirateLightClient) pirateChain.getBlockchainProvider();
            return lightClient.getServerStatusSummary();
        }
        return "n/a";
    }

    private boolean needsWalletSwitch(byte[] entropyBytes, boolean isNullSeedWallet) {
        if (this.currentWallet == null) {
            return true;
        }
        if (!this.currentWallet.entropyBytesEqual(entropyBytes)) {
            return true;
        }
        return this.currentWallet.isNullSeedWallet() != isNullSeedWallet;
    }

    private void sleepInitRetryDelay() {
        try {
            Thread.sleep(WALLET_INIT_RETRY_DELAY_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void run() {
        Thread.currentThread().setName("Pirate Chain Wallet Controller");
        Thread.currentThread().setPriority(MIN_PRIORITY);
        this.controllerThread = Thread.currentThread();
        // Sync watchdog disabled: never force-stop long syncs.

        try {
            while (running && !Controller.isStopping()) {
                Thread.sleep(1000);

                // Wait until we have a request to load the wallet or an active wallet to sync
                if (!shouldLoadWallet && this.currentWallet == null) {
                    this.logSyncSkip("no wallet requested or active");
                    continue;
                }

                if (!LiteWalletJni.isLoaded()) {
                    this.loadLibrary();

                    // If still not loaded, sleep to prevent too many requests
                    if (!LiteWalletJni.isLoaded()) {
                        this.logSyncSkip("JNI library not loaded");
                        Thread.sleep(5 * 1000);
                        continue;
                    }
                }

                // Wallet is downloaded, so clear the status
                this.updateLoadStatus(null);

                PirateWallet wallet = this.currentWallet;
                if (wallet == null) {
                    // Nothing to do yet
                    this.logSyncSkip("wallet not initialized");
                    continue;
                }
                if (wallet.isNullSeedWallet()) {
                    // Don't sync the null seed wallet
                    this.logSyncSkip("null-seed wallet is not synced");
                    continue;
                }

                if (this.isSwitching()) {
                    this.logSyncSkip("wallet switching in progress");
                    continue;
                }

                if (!this.acquireWalletLock(0)) {
                    this.logSyncSkip("wallet lock busy");
                    continue;
                }

                boolean syncStarted = false;
                try {
                    if (this.isSwitching()) {
                        this.logSyncSkip("wallet switching in progress");
                        continue;
                    }

                    if (this.walletReconnectRequested) {
                        if (!this.claimSwitching()) {
                            this.logSyncSkip("wallet switching in progress");
                            continue;
                        }
                        try {
                            if (this.lastEntropyBytes == null) {
                                LOGGER.info("Pirate wallet reconnect requested but no entropy available");
                                this.walletReconnectRequested = false;
                                continue;
                            }
                            this.closeCurrentWallet();
                            LOGGER.info("Reconnecting Pirate wallet to current lightwallet server...");
                            this.currentWallet = new PirateWallet(this.lastEntropyBytes, this.lastIsNullSeedWallet);
                            LOGGER.info(
                                    "Pirate wallet instance reconnected (ready={}, initialized={}, synchronized={})",
                                    this.currentWallet.isReady(),
                                    this.currentWallet.isInitialized(),
                                    this.currentWallet.isSynchronized());
                            if (!this.currentWallet.isReady()) {
                                this.currentWallet = null;
                                LOGGER.info("Pirate wallet reconnect failed: wallet not ready after initialization");
                            } else {
                                this.walletReconnectRequested = false;
                            }
                        } catch (IOException e) {
                            LOGGER.info("Pirate wallet reconnect failed: {}", e.getMessage());
                        } finally {
                            this.releaseSwitchingClaim();
                        }
                        if (this.currentWallet == null) {
                            this.logSyncSkip("wallet not initialized");
                            continue;
                        }
                    }

                    synchronized (this.syncMonitor) {
                        this.syncInProgress = true;
                        this.syncStartTimeMs = System.currentTimeMillis();
                    }
                    syncStarted = true;
                    LOGGER.info("Pirate lightwallet status: {}", this.getLightwalletStatusSummary());

                    long infoStartMs = System.currentTimeMillis();
                    String infoResponse = LiteWalletJni.execute("info", "");
                    if (infoResponse == null || infoResponse.trim().isEmpty()) {
                        LOGGER.info("Pirate wallet info returned empty response");
                    } else {
                        long infoElapsedMs = System.currentTimeMillis() - infoStartMs;
                        LOGGER.info("Pirate wallet info received in {} ms ({} chars)", infoElapsedMs,
                                infoResponse.length());
                        try {
                            JSONObject infoJson = new JSONObject(infoResponse);
                            long serverHeight = infoJson.optLong("latest_block_height", -1L);
                            String chainName = infoJson.optString("chain_name", null);
                            if (chainName != null && !chainName.isEmpty()) {
                                LOGGER.info("Pirate wallet server chain name: {}", chainName);
                            }
                            int configuredBirthday = Settings.getInstance().getArrrDefaultBirthday();
                            if (serverHeight > 0 && configuredBirthday > 0 && serverHeight < configuredBirthday) {
                                LOGGER.info(
                                        "Pirate wallet server height {} below configured birthday {} - switching server",
                                        serverHeight,
                                        configuredBirthday);
                                this.rotateLightwalletServer("server behind configured birthday");
                                continue;
                            }
                        } catch (JSONException e) {
                            LOGGER.info("Unable to parse Pirate wallet info response: {}", e.getMessage());
                        }
                    }

                    LOGGER.info("Syncing Pirate Chain wallet...");
                    String response;
                    long syncStartMs = System.currentTimeMillis();
                    try {
                        response = LiteWalletJni.execute("sync", "");
                    } catch (RuntimeException e) {
                        LOGGER.info("Pirate wallet sync call failed: {}", e.getMessage());
                        response = null;
                    }
                    long syncElapsedMs = System.currentTimeMillis() - syncStartMs;
                    if (syncElapsedMs >= SYNC_CALL_WARN_MS) {
                        LOGGER.warn("Pirate wallet sync call took {} ms", syncElapsedMs);
                    } else {
                        LOGGER.info("Pirate wallet sync call finished in {} ms", syncElapsedMs);
                    }
                    if (response == null || response.trim().isEmpty()) {
                        LOGGER.info("Pirate wallet sync returned empty response");
                    } else {
                        LOGGER.info("Pirate wallet sync response: {}", response);
                    }
                    LOGGER.info("Pirate lightwallet status after sync call: {}", this.getLightwalletStatusSummary());

                    if (response != null && !response.trim().isEmpty()) {
                        try {
                            JSONObject json = new JSONObject(response);
                            if (json.has("result")) {
                                String result = json.getString("result");

                                // We may have to set wallet to ready if this is the first ever successful sync
                                if (Objects.equals(result, "success")) {
                                    this.currentWallet.setReady(true);
                                } else {
                                    String reason = json.optString("reason", null);
                                    if (reason != null && !reason.isEmpty()) {
                                        LOGGER.info("Pirate wallet sync reported result {}: {}", result, reason);
                                    } else {
                                        LOGGER.info("Pirate wallet sync reported result {}", result);
                                    }
                                    boolean stopRequested = this.syncStopRequested.getAndSet(false);
                                    boolean interrupted = reason != null && reason.toLowerCase().contains("interupted");
                                    if (stopRequested || interrupted) {
                                        LOGGER.info("Skipping Pirate lightwallet server rotation after requested sync stop");
                                    } else {
                                        this.rotateLightwalletServer("sync failure");
                                    }
                                }
                            }
                        } catch (JSONException e) {
                            LOGGER.info("Unable to interpret JSON", e);
                        } catch (RuntimeException e) {
                            LOGGER.info("Unable to interpret sync response: {}", e.getMessage());
                        }
                    }

                if (this.forceSaveAfterSync.getAndSet(false)) {
                    LOGGER.info("Saving Pirate wallet cache after forced sync stop");
                    this.saveCurrentWallet();
                }
                } finally {
                    if (syncStarted) {
                        synchronized (this.syncMonitor) {
                            this.syncInProgress = false;
                            this.syncStartTimeMs = 0L;
                            this.syncMonitor.notifyAll();
                        }
                        this.syncStopRequested.set(false);
                    }
                    this.releaseWalletLockIfHeld();
                }

                // Rate limit sync attempts
                Thread.sleep(30000);

                // Save wallet if needed
                long now = this.getNowMillis();
                if (now - SAVE_INTERVAL >= this.lastSaveTime) {
                    this.saveCurrentWallet();
                }
                if (this.saveRequested.getAndSet(false)) {
                    this.saveCurrentWallet();
                }
            }
        } catch (InterruptedException e) {
            // Fall-through to exit
        }
    }

    public void shutdown() {
        // Save the wallet
        this.stopSyncIfRunning(SYNC_STOP_TIMEOUT_MS);
        this.waitForSyncIdle(SYNC_STOP_TIMEOUT_MS);
        this.saveCurrentWallet();

        this.running = false;
        this.syncWatchdogExecutor.shutdownNow();
        this.interrupt();
    }

    private long getNowMillis() {
        Long now = NTP.getTime();
        return now != null ? now : System.currentTimeMillis();
    }

    private void startSyncWatchdog() {
        if (!this.syncWatchdogStarted.compareAndSet(false, true)) {
            return;
        }
        this.syncWatchdogExecutor.scheduleWithFixedDelay(() -> {
            try {
                if (!this.syncInProgress || this.syncStartTimeMs <= 0L) {
                    return;
                }
                long elapsedMs = System.currentTimeMillis() - this.syncStartTimeMs;
                if (elapsedMs < SYNC_FORCE_STOP_MS || this.syncStopRequested.get()) {
                    return;
                }
                LOGGER.warn("Pirate wallet sync running {} ms; requesting stop to flush cache", elapsedMs);
                this.syncStopRequested.set(true);
                this.forceSaveAfterSync.set(true);
                this.stopSyncIfRunning(SYNC_STOP_TIMEOUT_MS);
            } catch (Exception e) {
                LOGGER.info("Pirate wallet sync watchdog error: {}", e.getMessage());
            }
        }, SYNC_WATCHDOG_INTERVAL_MS, SYNC_WATCHDOG_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    // QDN & wallet libraries

    private void loadLibrary() throws InterruptedException {
        try (final Repository repository = RepositoryManager.getRepository()) {

            this.updateLoadStatus("Loading Pirate Chain wallet library...");

            // Check if architecture is supported
            String libFileName = PirateChainWalletController.getRustLibFilename();
            if (libFileName == null) {
                String osName = System.getProperty("os.name");
                String osArchitecture = System.getProperty("os.arch");
                this.updateLoadStatus(String.format("Unsupported architecture (%s %s)", osName, osArchitecture));
                return;
            }

            // Check if the library exists in the wallets folder
            Path libDirectory = PirateChainWalletController.getRustLibOuterDirectory();
            Path libPath = Paths.get(libDirectory.toString(), libFileName);
            if (Files.exists(libPath)) {
                // Already downloaded; we can load the library right away
                this.updateLoadStatus("Loading cached Pirate Chain library from disk...");
                LiteWalletJni.loadLibrary();
                this.onLibraryLoaded();
                return;
            }

            // Library not found, so check if we've fetched the resource from QDN
            ArbitraryTransactionData t = this.getTransactionData(repository);
            if (t == null || t.getService() == null) {
                // Can't find the transaction - maybe on a different chain?
                this.updateLoadStatus("Waiting for Pirate Chain library publish transaction to appear on QDN...");
                return;
            }

            // Wait until we have a sufficient number of peers to attempt QDN downloads
            List<Peer> handshakedPeers = Network.getInstance().getImmutableHandshakedPeers();
            if (handshakedPeers.size() < Settings.getInstance().getMinBlockchainPeers()) {
                // Wait for more peers
                this.updateLoadStatus("Searching for peers...");
                return;
            }

            // Build resource
            ArbitraryDataReader arbitraryDataReader = new ArbitraryDataReader(t.getName(),
                    ArbitraryDataFile.ResourceIdType.NAME, t.getService(), t.getIdentifier());
            try {
                arbitraryDataReader.loadSynchronously(false);
            } catch (MissingDataException e) {
                LOGGER.info("Missing data when loading Pirate Chain library");
            }

            // Check its status
            ArbitraryResourceStatus status = ArbitraryTransactionUtils.getStatus(
                    t.getService(), t.getName(), t.getIdentifier(), false, true);

            if (status.getStatus() != ArbitraryResourceStatus.Status.READY) {
                LOGGER.info("Not ready yet: {}", status.getTitle());
                this.updateLoadStatus(
                        String.format("Downloading files from QDN... (%d / %d)", status.getLocalChunkCount(),
                                status.getTotalChunkCount()));
                return;
            }

            // Files are downloaded, so copy the necessary files to the wallets folder
            // Delete the wallets/*/lib directory first, in case earlier versions of the
            // wallet are present
            Path walletsLibDirectory = PirateChainWalletController.getWalletsLibDirectory();
            if (Files.exists(walletsLibDirectory)) {
                FilesystemUtils.safeDeleteDirectory(walletsLibDirectory, false);
            }
            Files.createDirectories(libDirectory);
            this.updateLoadStatus("Copying Pirate Chain library into wallets folder...");
            FileUtils.copyDirectory(arbitraryDataReader.getFilePath().toFile(), libDirectory.toFile());

            // Clear reader cache so only one copy exists
            ArbitraryDataResource resource = new ArbitraryDataResource(t.getName(),
                    ArbitraryDataFile.ResourceIdType.NAME, t.getService(), t.getIdentifier());
            resource.deleteCache();

            // Finally, load the library
            LiteWalletJni.loadLibrary();
            this.onLibraryLoaded();

        } catch (DataException e) {
            LOGGER.error("Repository issue when loading Pirate Chain library", e);
            this.updateLoadStatus("Repository issue while loading Pirate Chain library");
        } catch (IOException e) {
            LOGGER.error("Error when loading Pirate Chain library", e);
            this.updateLoadStatus("IO error while loading Pirate Chain library");
        }
    }

    private void onLibraryLoaded() {
        this.shouldLoadWallet = false;
        this.updateLoadStatus("Pirate Chain library loaded and ready");
    }

    private ArbitraryTransactionData getTransactionData(Repository repository) {
        try {
            byte[] signature = Base58.decode(qdnWalletSignature);
            TransactionData transactionData = repository.getTransactionRepository().fromSignature(signature);
            if (!(transactionData instanceof ArbitraryTransactionData))
                return null;

            ArbitraryTransaction arbitraryTransaction = new ArbitraryTransaction(repository, transactionData);
            if (arbitraryTransaction != null) {
                return (ArbitraryTransactionData) arbitraryTransaction.getTransactionData();
            }

            return null;
        } catch (DataException e) {
            return null;
        }
    }

    public static String getRustLibFilename() {
        String osName = System.getProperty("os.name");
        String osArchitecture = System.getProperty("os.arch");

        if (osName.equals("Mac OS X") && osArchitecture.equals("x86_64")) {
            return "librust-macos-x86_64.dylib";
        } else if (osName.equals("Mac OS X") && osArchitecture.equals("aarch64")) {
            return "librust-macos-aarch64.dylib";
        } else if ((osName.equals("Linux") || osName.equals("FreeBSD")) && osArchitecture.equals("aarch64")) {
            return "librust-linux-aarch64.so";
        } else if ((osName.equals("Linux") || osName.equals("FreeBSD")) && osArchitecture.equals("amd64")) {
            return "librust-linux-x86_64.so";
        } else if (osName.contains("Windows") && osArchitecture.equals("amd64")) {
            return "librust-windows-x86_64.dll";
        }

        return null;
    }

    public static Path getWalletsLibDirectory() {
        return Paths.get(Settings.getInstance().getWalletsPath(), "PirateChain", "lib");
    }

    public static Path getRustLibOuterDirectory() {
        String sigPrefix = qdnWalletSignature.substring(0, 8);
        return Paths.get(Settings.getInstance().getWalletsPath(), "PirateChain", "lib", sigPrefix);
    }

    // Wallet functions

    public boolean initWithEntropy58(String entropy58) {
        return this.initWithEntropy58(entropy58, false);
    }

    public boolean initNullSeedWallet() {
        return this.initWithEntropy58(NULL_SEED_ENTROPY58, true);
    }

    private boolean initWithEntropy58(String entropy58, boolean isNullSeedWallet) {
        try {
            this.beginWalletUse(entropy58, isNullSeedWallet, false, false);
            return true;
        } catch (ForeignBlockchainException e) {
            return false;
        } finally {
            this.endWalletUse();
        }
    }

    public void beginWalletUse(String entropy58, boolean isNullSeedWallet, boolean requireSync,
            boolean requireNotNullSeed)
            throws ForeignBlockchainException {
        // If the JNI library isn't loaded yet then we can't proceed
        if (!LiteWalletJni.isLoaded()) {
            this.shouldLoadWallet = true;
            LOGGER.info("Pirate wallet init requested but JNI library is not loaded yet");
            throw new ForeignBlockchainException("Pirate wallet isn't initialized yet");
        }

        if (entropy58 == null) {
            throw new ForeignBlockchainException("Invalid entropy bytes");
        }

        byte[] entropyBytes = Base58.decode(entropy58);
        if (entropyBytes == null || entropyBytes.length != 32) {
            throw new ForeignBlockchainException("Invalid entropy bytes");
        }
        this.lastEntropyBytes = entropyBytes;
        this.lastIsNullSeedWallet = isNullSeedWallet;

        boolean needsSwitch = this.needsWalletSwitch(entropyBytes, isNullSeedWallet);
        boolean claimedSwitching = false;
        if (!needsSwitch && this.isSwitching() && !this.isSwitchingByCurrentThread()) {
            throw new ForeignBlockchainException("Wallet switch in progress");
        }
        if (needsSwitch) {
            LOGGER.info("Pirate wallet switch required (nullSeed={})", isNullSeedWallet);
            claimedSwitching = this.claimSwitching();
            if (!claimedSwitching) {
                throw new ForeignBlockchainException("Wallet switch in progress");
            }
            this.stopSyncIfRunning(SYNC_STOP_TIMEOUT_MS);
        } else if (this.isSwitchingByCurrentThread()) {
            claimedSwitching = this.claimSwitching();
        }

        long timeoutMs = needsSwitch ? SWITCH_LOCK_TIMEOUT_MS : WALLET_LOCK_TIMEOUT_MS;
        if (!this.acquireWalletLock(timeoutMs)) {
            if (claimedSwitching) {
                this.releaseSwitchingClaim();
            }
            if (this.syncInProgress) {
                LOGGER.info("Pirate wallet init blocked: sync in progress");
                throw new ForeignBlockchainException("Sync in progress. Please try again later.");
            }
            if (this.isSwitching()) {
                LOGGER.info("Pirate wallet init blocked: wallet switch in progress");
                throw new ForeignBlockchainException("Wallet switch in progress");
            }
            LOGGER.info("Pirate wallet init blocked: wallet busy");
            throw new ForeignBlockchainException("Wallet busy. Please try again later.");
        }

        try {
            boolean needsSwitchLocked = this.needsWalletSwitch(entropyBytes, isNullSeedWallet);
            if (needsSwitchLocked) {
                long now = System.currentTimeMillis();
                if (this.lastWalletInitFailureMs > 0L
                        && now - this.lastWalletInitFailureMs < WALLET_INIT_RETRY_DELAY_MS) {
                    throw new ForeignBlockchainException("Wallet init cooling down. Please try again shortly.");
                }
                this.closeCurrentWallet();
                LOGGER.info("Creating Pirate wallet instance (nullSeed={})", isNullSeedWallet);
                PirateWallet wallet = new PirateWallet(entropyBytes, isNullSeedWallet);
                while (!wallet.isReady() && this.walletInitRetryCount < WALLET_INIT_MAX_RETRIES) {
                    this.walletInitRetryCount++;
                    this.lastWalletInitFailureMs = System.currentTimeMillis();
                    LOGGER.info(
                            "Pirate wallet init failed; retrying same server (attempt {}/{}) after delay",
                            this.walletInitRetryCount,
                            WALLET_INIT_MAX_RETRIES);
                    this.sleepInitRetryDelay();
                    wallet = new PirateWallet(entropyBytes, isNullSeedWallet);
                }
                if (!wallet.isReady()) {
                    LOGGER.info("Pirate wallet init failed after retries; switching server");
                    this.rotateLightwalletServer("wallet init failed");
                    this.walletInitRetryCount = 0;
                    this.lastWalletInitFailureMs = System.currentTimeMillis();
                    this.sleepInitRetryDelay();
                    wallet = new PirateWallet(entropyBytes, isNullSeedWallet);
                }
                this.currentWallet = wallet.isReady() ? wallet : null;
                if (this.currentWallet != null) {
                    this.walletInitRetryCount = 0;
                }
                if (this.currentWallet != null) {
                    LOGGER.info(
                            "Pirate wallet instance created (ready={}, initialized={}, synchronized={})",
                            this.currentWallet.isReady(),
                            this.currentWallet.isInitialized(),
                            this.currentWallet.isSynchronized());
                }
                if (this.currentWallet == null) {
                    LOGGER.info("Pirate wallet init failed after retry: wallet not ready");
                    this.lastWalletInitFailureMs = System.currentTimeMillis();
                    throw new ForeignBlockchainException("Pirate wallet isn't initialized yet");
                }
            }

            this.ensureInitialized();
            if (requireNotNullSeed) {
                this.ensureNotNullSeed();
            }
            if (requireSync) {
                this.ensureSynchronized();
            }
        } catch (ForeignBlockchainException e) {
            this.releaseWalletLockIfHeld();
            if (claimedSwitching) {
                this.releaseSwitchingClaim();
            }
            throw e;
        } catch (IOException e) {
            this.releaseWalletLockIfHeld();
            if (claimedSwitching) {
                this.releaseSwitchingClaim();
            }
            throw new ForeignBlockchainException("Unable to initialize wallet: " + e.getMessage());
        } catch (RuntimeException e) {
            this.releaseWalletLockIfHeld();
            if (claimedSwitching) {
                this.releaseSwitchingClaim();
            }
            throw e;
        }
    }

    public void beginNullSeedWalletUse(boolean requireSync) throws ForeignBlockchainException {
        this.beginWalletUse(NULL_SEED_ENTROPY58, true, requireSync, false);
    }

    public void endWalletUse() {
        this.releaseWalletLockIfHeld();
        this.releaseSwitchingClaim();
    }

    private void saveCurrentWallet() {
        if (Thread.currentThread() != this.controllerThread) {
            this.saveRequested.set(true);
            return;
        }
        if (this.syncInProgress || this.isSwitching()) {
            this.saveRequested.set(true);
            return;
        }
        this.saveCurrentWalletInternal();
    }

    private void saveCurrentWalletInternal() {
        if (this.currentWallet == null) {
            // Nothing to do
            return;
        }
        boolean lockedHere = false;
        if (!this.walletLock.isHeldByCurrentThread()) {
            lockedHere = this.acquireWalletLock(WALLET_LOCK_TIMEOUT_MS);
            if (!lockedHere) {
                return;
            }
        }
        try {
            if (this.currentWallet == null) {
                return;
            }
            if (this.currentWallet.save()) {
                this.lastSaveTime = this.getNowMillis();
            }
        } catch (IOException e) {
            LOGGER.info("Unable to save wallet: {}", e.getMessage());
        } finally {
            if (lockedHere) {
                this.releaseWalletLockIfHeld();
            }
        }
    }

    public PirateWallet getCurrentWallet() {
        return this.currentWallet;
    }

    private void closeCurrentWallet() {
        this.saveCurrentWallet();
        this.currentWallet = null;
    }

    public void ensureInitialized() throws ForeignBlockchainException {
        if (!LiteWalletJni.isLoaded() || this.currentWallet == null || !this.currentWallet.isInitialized()) {
            throw new ForeignBlockchainException("Pirate wallet isn't initialized yet");
        }
    }

    public void ensureNotNullSeed() throws ForeignBlockchainException {
        // Safety check to make sure funds aren't sent to a null seed wallet
        if (this.currentWallet == null || this.currentWallet.isNullSeedWallet()) {
            throw new ForeignBlockchainException("Invalid wallet");
        }
    }

    public void ensureSynchronized() throws ForeignBlockchainException {
        if (this.isSwitching() && !this.isSwitchingByCurrentThread()) {
            throw new ForeignBlockchainException("Wallet switch in progress");
        }
        if (this.syncInProgress) {
            throw new ForeignBlockchainException("Sync in progress. Please try again later.");
        }
        if (!this.walletLock.isHeldByCurrentThread()) {
            throw new ForeignBlockchainException("Wallet busy. Please try again later.");
        }
        if (this.currentWallet == null || !this.currentWallet.isSynchronized()) {
            throw new ForeignBlockchainException("Wallet isn't synchronized yet");
        }

        String response = LiteWalletJni.execute("syncStatus", "");
        if (response == null || response.trim().isEmpty()) {
            throw new ForeignBlockchainException("Sync status unavailable. Please try again later.");
        }
        try {
            JSONObject json = new JSONObject(response);
            if (json.optBoolean("syncing", false)) {
                throw new ForeignBlockchainException("Sync in progress. Please try again later.");
            }
            boolean inProgress = json.optBoolean("in_progress", false);
            if (inProgress) {
                String progress = this.formatSyncProgress(json);
                String progressSuffix = progress != null ? String.format(" (%s)", progress) : "";
                throw new ForeignBlockchainException(
                        String.format("Sync in progress%s. Please try again later.", progressSuffix));
            }
        } catch (JSONException e) {
            throw new ForeignBlockchainException("Sync status unavailable. Please try again later.");
        }
    }

    private Long fetchChainHeight() {
        String response = LiteWalletJni.execute("info", "");
        if (response == null || response.trim().isEmpty()) {
            return null;
        }
        try {
            JSONObject json = new JSONObject(response);
            if (json.has("latest_block_height")) {
                return json.getLong("latest_block_height");
            }
        } catch (JSONException e) {
            // Fall through to return null.
        }
        return null;
    }

    private String formatSyncProgress(JSONObject statusJson) {
        long scannedHeight = statusJson.optLong("scanned_height", -1);
        if (scannedHeight >= 0) {
            Long chainHeight = this.fetchChainHeight();
            if (chainHeight != null && chainHeight >= 0) {
                long currentHeight = Math.min(scannedHeight, chainHeight);
                return String.format("%d / %d", currentHeight, chainHeight);
            }
        }

        long syncedBlocks = statusJson.optLong("synced_blocks", -1);
        long endBlock = statusJson.optLong("end_block", -1);
        long startBlock = statusJson.optLong("start_block", -1);

        if (endBlock > 0 && syncedBlocks >= 0) {
            long currentHeight = endBlock - 1 + syncedBlocks;
            if (startBlock > 0 && currentHeight > startBlock) {
                currentHeight = startBlock;
            }

            Long chainHeight = this.fetchChainHeight();
            if (chainHeight != null && chainHeight >= 0) {
                if (currentHeight > chainHeight) {
                    currentHeight = chainHeight;
                }
                return String.format("%d / %d", currentHeight, chainHeight);
            }
        }

        long totalBlocks = statusJson.optLong("total_blocks", -1);
        if (syncedBlocks >= 0 && totalBlocks >= 0) {
            return String.format("%d / %d", syncedBlocks, totalBlocks);
        }

        return null;
    }

    private String formatSyncStatus(PirateWallet wallet) {
        return this.formatSyncStatus(wallet, this.fetchSyncStatusResponse());
    }

    private String formatSyncStatus(PirateWallet wallet, String syncStatusResponse) {
        if (syncStatusResponse != null) {
            LOGGER.info("Pirate wallet syncStatus response: {}", syncStatusResponse);
        }
        if (syncStatusResponse == null || syncStatusResponse.trim().isEmpty()) {
            LOGGER.info("Pirate wallet syncStatus returned empty response");
            return this.syncInProgress ? "Sync in progress" : "Sync status unavailable";
        }

        JSONObject json;
        try {
            json = new JSONObject(syncStatusResponse);
        } catch (JSONException e) {
            LOGGER.info("Pirate wallet syncStatus returned invalid JSON: {}", syncStatusResponse);
            return "Sync status unavailable";
        }

        boolean inProgress = json.optBoolean("in_progress", false);
        if (this.syncInProgress && !inProgress) {
            LOGGER.info("Pirate wallet sync stalled (no in-progress marker); cache reset disabled");
            return "Sync in progress";
        }
        if (inProgress) {
            String progress = this.formatSyncProgress(json);
            if (progress != null) {
                LOGGER.info("Pirate wallet sync progress: {}", progress);
                return String.format("Sync in progress (%s)", progress);
            }
            LOGGER.info("Pirate wallet sync in progress without detailed progress");
            return "Sync in progress";
        }

        if (this.syncInProgress) {
            String progress = this.formatSyncProgress(json);
            if (progress != null) {
                LOGGER.info("Pirate wallet sync progress: {}", progress);
                return String.format("Sync in progress (%s)", progress);
            }
            return "Sync in progress";
        }

        if (wallet != null && wallet.isSynchronized()) {
            return "Synchronized";
        }

        return "Initializing wallet...";
    }

    private String buildSyncStatusJson(String status, String rawResponse) {
        JSONObject payload = new JSONObject();
        payload.put("status", status);
        if (rawResponse != null && !rawResponse.trim().isEmpty()) {
            try {
                payload.put("raw", new JSONObject(rawResponse));
            } catch (JSONException e) {
                payload.put("raw", rawResponse);
            }
        }
        return payload.toString();
    }

    public String getSyncStatus() {
        PirateWallet wallet = this.currentWallet;
        if (this.isSwitching() && !this.isSwitchingByCurrentThread()) {
            return this.formatSwitchingStatus();
        }
        if (wallet == null || !wallet.isInitialized()) {
            if (this.loadStatus != null) {
                LOGGER.info("Pirate wallet sync status: {}", this.loadStatus);
                return this.loadStatus;
            }

            LOGGER.info("Pirate wallet sync status: initializing (wallet not ready)");
            return "Not initialized yet";
        }

        if (this.syncInProgress) {
            this.logSyncRunningIfNeeded();
            String status = this.formatSyncStatus(wallet);
            if ("Initializing wallet...".equals(status)) {
                status = "Sync in progress";
            }
            LOGGER.info("Pirate wallet sync status: {}", status);
            return status;
        }

        boolean lockedHere = false;
        if (!this.walletLock.isHeldByCurrentThread()) {
            lockedHere = this.acquireWalletLock(STATUS_LOCK_TIMEOUT_MS);
            if (!lockedHere) {
                if (this.syncInProgress) {
                    return this.formatSyncStatus(wallet);
                }
                if (this.isSwitching()) {
                    return this.formatSwitchingStatus();
                }
                return "Wallet busy";
            }
        }

        try {
            if (this.currentWallet == null || !this.currentWallet.isInitialized()) {
                if (this.loadStatus != null) {
                    LOGGER.info("Pirate wallet sync status: {}", this.loadStatus);
                    return this.loadStatus;
                }
                LOGGER.info("Pirate wallet sync status: initializing (wallet not ready)");
                return "Not initialized yet";
            }

            return this.formatSyncStatus(this.currentWallet);
        } finally {
            if (lockedHere) {
                this.releaseWalletLockIfHeld();
            }
        }
    }

    public String getSyncStatusJson() {
        PirateWallet wallet = this.currentWallet;
        if (this.isSwitching() && !this.isSwitchingByCurrentThread()) {
            return this.buildSyncStatusJson(this.formatSwitchingStatus(), null);
        }
        if (wallet == null || !wallet.isInitialized()) {
            if (this.loadStatus != null) {
                LOGGER.info("Pirate wallet sync status: {}", this.loadStatus);
                return this.buildSyncStatusJson(this.loadStatus, null);
            }

            LOGGER.info("Pirate wallet sync status: initializing (wallet not ready)");
            return this.buildSyncStatusJson("Not initialized yet", null);
        }

        String status;
        String rawResponse = this.fetchSyncStatusResponse();
        if (this.syncInProgress) {
            this.logSyncRunningIfNeeded();
            status = this.formatSyncStatus(wallet, rawResponse);
            if ("Initializing wallet...".equals(status)) {
                status = "Sync in progress";
            }
            LOGGER.info("Pirate wallet sync status: {}", status);
        } else {
            status = this.formatSyncStatus(wallet, rawResponse);
        }

        return this.buildSyncStatusJson(status, rawResponse);
    }

    private String fetchSyncStatusResponse() {
        if (!this.syncStatusSemaphore.tryAcquire()) {
            LOGGER.info("Pirate wallet syncStatus skipped: busy");
            return null;
        }
        try {
            return LiteWalletJni.execute("syncStatus", "");
        } catch (Exception e) {
            LOGGER.info("Pirate wallet syncStatus failed: {}", e.getClass().getSimpleName());
            return null;
        } finally {
            this.syncStatusSemaphore.release();
        }
    }

    public String getSyncStatusWithInit(String entropy58) throws ForeignBlockchainException {
        synchronized (this.statusLock) {
            this.initWithEntropy58(entropy58);
            return this.getSyncStatus();
        }
    }

    public String getSyncStatusJsonWithInit(String entropy58) throws ForeignBlockchainException {
        synchronized (this.statusLock) {
            this.initWithEntropy58(entropy58);
            return this.getSyncStatusJson();
        }
    }

}
