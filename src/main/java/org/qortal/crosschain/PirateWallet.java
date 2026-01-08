package org.qortal.crosschain;

import com.rust.litewalletjni.LiteWalletJni;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.util.encoders.Base64;
import org.bouncycastle.util.encoders.DecoderException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.qortal.api.resource.CrossChainUtils;
import org.qortal.controller.PirateChainWalletController;
import org.qortal.crypto.Crypto;
import org.qortal.settings.Settings;
import org.qortal.utils.Base58;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

public class PirateWallet {

    protected static final Logger LOGGER = LogManager.getLogger(PirateWallet.class);

    private byte[] entropyBytes;
    private final boolean isNullSeedWallet;
    private String seedPhrase;
    private boolean ready = false;

    private String params;
    private String saplingOutput64;
    private String saplingSpend64;

    private final static String COIN_PARAMS_FILENAME = "coinparams.json";
    private final static String SAPLING_OUTPUT_FILENAME = "saplingoutput_base64";
    private final static String SAPLING_SPEND_FILENAME = "saplingspend_base64";

    public PirateWallet(byte[] entropyBytes, boolean isNullSeedWallet) throws IOException {
        this.entropyBytes = entropyBytes;
        this.isNullSeedWallet = isNullSeedWallet;

        Path libDirectory = PirateChainWalletController.getRustLibOuterDirectory();
        if (!Files.exists(Paths.get(libDirectory.toString(), COIN_PARAMS_FILENAME))) {
            return;
        }

        this.params = Files.readString(Paths.get(libDirectory.toString(), COIN_PARAMS_FILENAME));
        this.saplingOutput64 = Files.readString(Paths.get(libDirectory.toString(), SAPLING_OUTPUT_FILENAME));
        this.saplingSpend64 = Files.readString(Paths.get(libDirectory.toString(), SAPLING_SPEND_FILENAME));

        this.ready = this.initialize();
    }

    private boolean initialize() {
        try {
            LiteWalletJni.initlogging();

            if (this.entropyBytes == null) {
                return false;
            }

            // Pick a random server
            BitcoinyBlockchainProvider provider = PirateChain.getInstance().blockchainProvider;
            ChainableServer server = ensureServerAvailable(provider);
            if (server == null) {
                LOGGER.info("Unable to initialize Pirate wallet: no lightwallet server available");
                return false;
            }
            String serverUri = String.format("https://%s:%d/", server.getHostName(), server.getPort());
            LOGGER.info("Initializing Pirate wallet using server {} (nullSeed={})", serverUri, this.isNullSeedWallet);

            // Pirate library uses base64 encoding
            String entropy64 = Base64.toBase64String(this.entropyBytes);

            // Derive seed phrase from entropy bytes
            String inputSeedResponse = LiteWalletJni.getseedphrasefromentropyb64(entropy64);
            JSONObject inputSeedJson = parseJsonObject(inputSeedResponse, "getseedphrasefromentropyb64");
            if (inputSeedJson == null) {
                LOGGER.info("Unable to initialize Pirate Chain wallet: seed phrase response was not valid JSON");
                return false;
            }
            String inputSeedPhrase = null;
            if (inputSeedJson.has("seedPhrase")) {
                inputSeedPhrase = inputSeedJson.getString("seedPhrase");
            }

            int configuredBirthday = Settings.getInstance().getArrrDefaultBirthday();
            boolean forceFullRescan = !this.isNullSeedWallet && configuredBirthday <= 1;

            String wallet = this.load();
            boolean loadedFromCache = wallet != null;
            if (wallet != null && forceFullRescan) {
                LOGGER.info("Forcing full rescan due to configured birthday {}", configuredBirthday);
                this.deleteWalletCache();
                wallet = null;
                loadedFromCache = false;
            }
            if (wallet == null) {
                // Wallet doesn't exist, so create a new one
                LOGGER.info("Creating new Pirate wallet (birthday={})", configuredBirthday);

                int birthday = configuredBirthday;
                if (this.isNullSeedWallet) {
                    try {
                        // Attempt to set birthday to the current block for null seed wallets
                        birthday = PirateChain.getInstance().blockchainProvider.getCurrentHeight();
                    } catch (ForeignBlockchainException e) {
                        // Use the default height
                    }
                }

                // Initialize new wallet
                if (!this.initFromSeed(serverUri, inputSeedPhrase, birthday)) {
                    LOGGER.info("Pirate wallet initFromSeed failed (birthday={})", birthday);
                    return false;
                }
            } else {
                // Restore existing wallet
                String response = LiteWalletJni.initfromb64(serverUri, params, wallet, saplingOutput64, saplingSpend64);
                if (response != null && !isInitSuccess(response)) {
                    if (isBufferFillError(response)) {
                        LOGGER.info("Pirate wallet init reported buffer error; backing up cache and retrying");
                        this.backupAndDeleteWalletCache();
                        response = LiteWalletJni.initfromb64(serverUri, params, wallet, saplingOutput64,
                                saplingSpend64);
                    }
                    if (response != null && !isInitSuccess(response)) {
                        LOGGER.info("Unable to initialize Pirate Chain wallet at {}: {}", serverUri, response);
                        return false;
                    }
                }
                LOGGER.info("Loaded Pirate wallet from cache");
                this.seedPhrase = inputSeedPhrase;
            }

            // Check that we're able to communicate with the library
            Integer ourHeight = this.getHeight();
            if (ourHeight == null || ourHeight <= 0) {
                LOGGER.info("Pirate wallet height unavailable after init (height={})", ourHeight);
                return false;
            }
            LOGGER.info("Pirate wallet height after init: {}", ourHeight);

            if (!this.isNullSeedWallet && configuredBirthday > 1 && ourHeight < configuredBirthday) {
                if (loadedFromCache) {
                    LOGGER.warn("Pirate wallet height {} below configured birthday {}. Recreating wallet cache.",
                            ourHeight, configuredBirthday);
                    this.deleteWalletCache();
                    if (!this.initFromSeed(serverUri, inputSeedPhrase, configuredBirthday)) {
                        LOGGER.info("Pirate wallet re-init from seed failed (birthday={})", configuredBirthday);
                        return false;
                    }
                    ourHeight = this.getHeight();
                }

                if (ourHeight == null || ourHeight <= 0 || ourHeight < configuredBirthday) {
                    LOGGER.warn("Pirate wallet initialized below configured birthday {} (height {}).",
                            configuredBirthday, ourHeight);
                    return false;
                }
            }

            return true;

        } catch (IOException | JSONException | UnsatisfiedLinkError e) {
            LOGGER.info("Unable to initialize Pirate Chain wallet: {}", e.getMessage());
        }

        return false;
    }

    private boolean initFromSeed(String serverUri, String inputSeedPhrase, int birthday) {
        String birthdayString = String.format("%d", birthday);
        String outputSeedResponse = LiteWalletJni.initfromseed(serverUri, this.params, inputSeedPhrase, birthdayString,
                this.saplingOutput64, this.saplingSpend64); // Thread-safe.
        String outputSeedPhrase = parseSeedPhrase(outputSeedResponse, "initfromseed");
        if (outputSeedPhrase == null && isBufferFillError(outputSeedResponse)) {
            LOGGER.info("Pirate wallet initfromseed reported buffer error; backing up cache and retrying");
            this.backupAndDeleteWalletCache();
            outputSeedResponse = LiteWalletJni.initfromseed(serverUri, this.params, inputSeedPhrase, birthdayString,
                    this.saplingOutput64, this.saplingSpend64); // Thread-safe.
            outputSeedPhrase = parseSeedPhrase(outputSeedResponse, "initfromseed");
        }
        if (outputSeedPhrase == null && isWalletAlreadyExistsError(outputSeedResponse)) {
            LOGGER.info("Clearing litewallet cache after initfromseed reported existing wallet");
            this.deleteLitewalletCache();
            outputSeedResponse = LiteWalletJni.initfromseed(serverUri, this.params, inputSeedPhrase, birthdayString,
                    this.saplingOutput64, this.saplingSpend64); // Thread-safe.
            outputSeedPhrase = parseSeedPhrase(outputSeedResponse, "initfromseed");
        }
        if (outputSeedPhrase == null) {
            LOGGER.info("Unable to initialize Pirate Chain wallet: init response did not contain a seed phrase");
            return false;
        }

        // Ensure seed phrase in response matches supplied seed phrase
        if (inputSeedPhrase == null || !Objects.equals(inputSeedPhrase, outputSeedPhrase)) {
            LOGGER.info("Unable to initialize Pirate Chain wallet: seed phrases do not match, or are null");
            return false;
        }

        this.seedPhrase = outputSeedPhrase;
        return true;
    }

    private boolean isBufferFillError(String response) {
        return response != null && response.contains("failed to fill whole buffer");
    }

    private boolean isInitSuccess(String response) {
        if (response == null) {
            return false;
        }
        return response.contains("\"initialized\":true") || response.contains("\"initalized\":true");
    }

    private boolean isWalletAlreadyExistsError(String response) {
        if (response == null) {
            return false;
        }
        String normalized = response.toLowerCase(Locale.ROOT);
        return normalized.contains("wallet already exists");
    }

    private void deleteWalletCache() {
        Path walletPath = this.getCurrentWalletPath();
        boolean walletExists = Files.exists(walletPath);
        LOGGER.info(
                "Deleting Pirate wallet cache file {} (exists={}, walletsPath={})",
                walletPath,
                walletExists,
                Settings.getInstance().getWalletsPath());
        if (walletExists) {
            try {
                LOGGER.info("Pirate wallet cache file size={}, lastModified={}",
                        Files.size(walletPath),
                        Files.getLastModifiedTime(walletPath));
            } catch (IOException e) {
                LOGGER.info("Unable to read Pirate wallet cache metadata at {}: {}", walletPath, e.getMessage());
            }
        }
        try {
            Files.deleteIfExists(walletPath);
        } catch (IOException e) {
            LOGGER.info("Unable to delete Pirate Chain wallet cache at {}: {}", walletPath, e.getMessage());
        }
        boolean walletExistsAfterDelete = Files.exists(walletPath);
        LOGGER.info("Pirate wallet cache file exists after delete: {}", walletExistsAfterDelete);
        if (walletExistsAfterDelete) {
            Path backupPath = walletPath.resolveSibling(walletPath.getFileName() + ".bak");
            try {
                Files.move(walletPath, backupPath, StandardCopyOption.REPLACE_EXISTING);
                LOGGER.info("Moved Pirate wallet cache file to {}", backupPath);
            } catch (IOException e) {
                LOGGER.info("Unable to move Pirate wallet cache file to {}: {}", backupPath, e.getMessage());
            }
        }

        this.deleteLitewalletCache();
    }

    private void backupAndDeleteWalletCache() {
        Path walletPath = this.getCurrentWalletPath();
        if (walletPath == null) {
            return;
        }
        if (!Files.exists(walletPath)) {
            LOGGER.info("No Pirate wallet cache file to backup at {}", walletPath);
            return;
        }
        Path backupPath = walletPath.resolveSibling(".bak-" + walletPath.getFileName());
        try {
            Files.copy(walletPath, backupPath, StandardCopyOption.REPLACE_EXISTING);
            LOGGER.info("Backed up Pirate wallet cache file to {}", backupPath);
        } catch (IOException e) {
            LOGGER.info("Unable to backup Pirate wallet cache file to {}: {}", backupPath, e.getMessage());
        }
        try {
            Files.deleteIfExists(walletPath);
            LOGGER.info("Deleted Pirate wallet cache file after backup: {}", walletPath);
        } catch (IOException e) {
            LOGGER.info("Unable to delete Pirate wallet cache file {}: {}", walletPath, e.getMessage());
        }
        this.deleteLitewalletCache();
    }

    private ChainableServer ensureServerAvailable(BitcoinyBlockchainProvider provider) {
        ChainableServer server = provider.getCurrentServer();
        if (server != null) {
            return server;
        }

        Set<ChainableServer> candidates = new HashSet<>(provider.getServers());
        candidates.removeAll(provider.getUselessServers());
        if (candidates.isEmpty()) {
            candidates = new HashSet<>(provider.getServers());
        }
        if (candidates.isEmpty()) {
            return null;
        }

        List<ChainableServer> candidateList = new ArrayList<>(candidates);
        ChainableServer candidate = candidateList.get(new Random().nextInt(candidateList.size()));
        LOGGER.info("Selecting Pirate lightwallet server {} (no current server)", candidate);
        try {
            provider.setCurrentServer(candidate, "PirateWallet");
        } catch (ForeignBlockchainException e) {
            LOGGER.info("Unable to set Pirate lightwallet server {}: {}", candidate, e.getMessage());
            return null;
        }
        return provider.getCurrentServer();
    }

    public void resetCache() {
        LOGGER.info("Clearing Pirate wallet cache");
        this.deleteWalletCache();
    }

    private void deleteLitewalletCache() {
        Path pirateDir = this.getLitewalletDataDirectory();
        Path defaultWalletPath = pirateDir.resolve("arrr-light-wallet.dat");
        LOGGER.info("Deleting litewallet cache file {} (exists={})", defaultWalletPath,
                Files.exists(defaultWalletPath));
        try {
            Files.deleteIfExists(defaultWalletPath);
        } catch (IOException e) {
            LOGGER.info("Unable to delete litewallet cache at {}: {}", defaultWalletPath, e.getMessage());
        }
        LOGGER.info("Litewallet cache file exists after delete: {}", Files.exists(defaultWalletPath));

        Path tempDir = pirateDir.resolve("temp");
        if (Files.isDirectory(tempDir)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(tempDir, "arrr-light-wallet-*.dat")) {
                for (Path path : stream) {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        LOGGER.info("Unable to delete litewallet temp cache at {}: {}", path, e.getMessage());
                    }
                }
            } catch (IOException e) {
                LOGGER.info("Unable to scan litewallet temp directory at {}: {}", tempDir, e.getMessage());
            }
        }

        Path qortalCachePath = Paths.get(Settings.getInstance().getWalletsPath(), "PirateChain", "lib");
        LOGGER.info("Deleting Pirate wallet lib cache path {} (exists={})", qortalCachePath,
                Files.exists(qortalCachePath));
        this.deletePathRecursive(qortalCachePath);
        LOGGER.info("Pirate wallet lib cache path exists after delete: {}", Files.exists(qortalCachePath));
    }

    private void deletePathRecursive(Path path) {
        if (path == null || !Files.exists(path)) {
            return;
        }
        try {
            if (Files.isDirectory(path)) {
                try (var walk = Files.walk(path)) {
                    walk.sorted(Comparator.reverseOrder()).forEach(entry -> {
                        try {
                            Files.deleteIfExists(entry);
                        } catch (IOException e) {
                            LOGGER.info("Unable to delete {}", entry);
                        }
                    });
                }
            } else {
                Files.deleteIfExists(path);
            }
        } catch (IOException e) {
            LOGGER.info("Unable to delete {}: {}", path, e.getMessage());
        }
    }

    private Path getLitewalletDataDirectory() {
        String osName = System.getProperty("os.name");
        String homeDir = System.getProperty("user.home");
        Path baseDir;

        if (osName != null && osName.contains("Windows")) {
            String appData = System.getenv("APPDATA");
            if (appData != null && !appData.isEmpty()) {
                baseDir = Paths.get(appData, "Pirate");
            } else if (homeDir != null && !homeDir.isEmpty()) {
                baseDir = Paths.get(homeDir, "AppData", "Roaming", "Pirate");
            } else {
                baseDir = Paths.get("Pirate");
            }
        } else if ("Mac OS X".equals(osName)) {
            if (homeDir != null && !homeDir.isEmpty()) {
                baseDir = Paths.get(homeDir, "Library", "Application Support", "Pirate");
            } else {
                baseDir = Paths.get("Pirate");
            }
        } else {
            if (homeDir != null && !homeDir.isEmpty()) {
                baseDir = Paths.get(homeDir, ".pirate");
            } else {
                baseDir = Paths.get(".pirate");
            }
        }

        PirateChain.PirateChainNet pirateChainNet = Settings.getInstance().getPirateChainNet();
        if (pirateChainNet == PirateChain.PirateChainNet.TEST3) {
            baseDir = baseDir.resolve("testnet3");
        } else if (pirateChainNet == PirateChain.PirateChainNet.REGTEST) {
            baseDir = baseDir.resolve("regtest");
        }

        return baseDir;
    }

    public boolean isReady() {
        return this.ready;
    }

    public void setReady(boolean ready) {
        this.ready = ready;
    }

    public boolean entropyBytesEqual(byte[] testEntropyBytes) {
        return Arrays.equals(testEntropyBytes, this.entropyBytes);
    }

    private void encrypt() {
        if (this.isEncrypted()) {
            // Nothing to do
            return;
        }

        String encryptionKey = this.getEncryptionKey();
        if (encryptionKey == null) {
            // Can't encrypt without a key
            return;
        }

        this.doEncrypt(encryptionKey);
    }

    private void decrypt() {
        if (!this.isEncrypted()) {
            // Nothing to do
            return;
        }

        String encryptionKey = this.getEncryptionKey();
        if (encryptionKey == null) {
            // Can't encrypt without a key
            return;
        }

        this.doDecrypt(encryptionKey);
    }

    public void unlock() {
        if (!this.isEncrypted()) {
            // Nothing to do
            return;
        }

        String encryptionKey = this.getEncryptionKey();
        if (encryptionKey == null) {
            // Can't encrypt without a key
            return;
        }

        this.doUnlock(encryptionKey);
    }

    public boolean save() throws IOException {
        if (!isInitialized()) {
            LOGGER.info("Error: can't save wallet, because no wallet it initialized");
            return false;
        }
        if (this.isNullSeedWallet()) {
            // Don't save wallets that have a null seed
            return false;
        }

        // Encrypt first (will do nothing if already encrypted)
        this.encrypt();

        String wallet64 = LiteWalletJni.save();
        byte[] wallet;
        try {
            wallet = Base64.decode(wallet64);
        } catch (DecoderException e) {
            LOGGER.info("Unable to decode wallet");
            return false;
        }
        if (wallet == null) {
            LOGGER.info("Unable to save wallet");
            return false;
        }

        Path walletPath = this.getCurrentWalletPath();
        Files.createDirectories(walletPath.getParent());
        Path tempPath = walletPath.resolveSibling(walletPath.getFileName().toString() + ".tmp");
        Files.write(tempPath, wallet, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        try {
            Files.move(tempPath, walletPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tempPath, walletPath, StandardCopyOption.REPLACE_EXISTING);
        }
        try {
            LOGGER.info("Saved Pirate Chain wallet to {} (size={})", walletPath, Files.size(walletPath));
        } catch (IOException e) {
            LOGGER.info("Saved Pirate Chain wallet to {}", walletPath);
        }

        return true;
    }

    public String load() throws IOException {
        if (this.isNullSeedWallet()) {
            // Don't load wallets that have a null seed
            return null;
        }
        Path walletPath = this.getCurrentWalletPath();
        if (!Files.exists(walletPath)) {
            return null;
        }
        byte[] wallet = Files.readAllBytes(walletPath);
        if (wallet == null) {
            return null;
        }
        String wallet64 = Base64.toBase64String(wallet);
        return wallet64;
    }

    private String getEntropyHash58() {
        if (this.entropyBytes == null) {
            return null;
        }
        byte[] entropyHash = Crypto.digest(this.entropyBytes);
        return Base58.encode(entropyHash);
    }

    public String getSeedPhrase() {
        return this.seedPhrase;
    }

    private String getEncryptionKey() {
        if (this.entropyBytes == null) {
            return null;
        }

        // Prefix the bytes with a (deterministic) string, to ensure that the resulting
        // hash is different
        String prefix = "ARRRWalletEncryption";

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            outputStream.write(prefix.getBytes(StandardCharsets.UTF_8));
            outputStream.write(this.entropyBytes);

        } catch (IOException e) {
            return null;
        }

        byte[] encryptionKeyHash = Crypto.digest(outputStream.toByteArray());
        return Base58.encode(encryptionKeyHash);
    }

    private Path getCurrentWalletPath() {
        String entropyHash58 = this.getEntropyHash58();
        String filename = String.format("wallet-%s.dat", entropyHash58);
        return Paths.get(Settings.getInstance().getWalletsPath(), "PirateChain", filename);
    }

    public boolean isInitialized() {
        return this.entropyBytes != null && this.ready;
    }

    public boolean isSynchronized() {
        Integer height = this.getHeight();
        Integer chainTip = this.getChainTip();

        if (height == null || chainTip == null) {
            return false;
        }

        // Assume synchronized if within 2 blocks of the chain tip
        return height >= (chainTip - 2);
    }

    private JSONObject parseJsonObject(String response, String context) {
        if (response == null) {
            LOGGER.info("Pirate wallet {} response was null", context);
            return null;
        }

        String trimmed = response.trim();
        if (trimmed.isEmpty()) {
            LOGGER.info("Pirate wallet {} response was empty", context);
            return null;
        }
        if (!trimmed.startsWith("{")) {
            LOGGER.info("Pirate wallet {} returned non-JSON response (length {})", context, trimmed.length());
            return null;
        }

        try {
            return new JSONObject(trimmed);
        } catch (JSONException e) {
            LOGGER.info("Pirate wallet {} returned invalid JSON: {}", context, e.getMessage());
            return null;
        }
    }

    private String parseSeedPhrase(String response, String context) {
        if (response == null) {
            LOGGER.info("Pirate wallet {} response was null", context);
            return null;
        }

        String trimmed = response.trim();
        if (trimmed.isEmpty()) {
            LOGGER.info("Pirate wallet {} response was empty", context);
            return null;
        }

        if (trimmed.startsWith("{")) {
            JSONObject json = parseJsonObject(trimmed, context);
            if (json == null) {
                return null;
            }
            if (json.has("seed")) {
                return json.getString("seed");
            }
            if (json.has("error")) {
                LOGGER.info("Pirate wallet {} error: {}", context, json.optString("error"));
                return null;
            }
            LOGGER.info("Pirate wallet {} response missing seed phrase", context);
            return null;
        }

        if (trimmed.startsWith("Error:")) {
            LOGGER.info("Pirate wallet {} error: {}", context, trimmed);
            return null;
        }

        return trimmed;
    }

    private JSONArray parseJsonArray(String response, String context) {
        if (response == null) {
            LOGGER.info("Pirate wallet {} response was null", context);
            return null;
        }

        String trimmed = response.trim();
        if (trimmed.isEmpty()) {
            LOGGER.info("Pirate wallet {} response was empty", context);
            return null;
        }
        if (!trimmed.startsWith("[")) {
            LOGGER.info("Pirate wallet {} returned non-JSON response (length {})", context, trimmed.length());
            return null;
        }

        try {
            return new JSONArray(trimmed);
        } catch (JSONException e) {
            LOGGER.info("Pirate wallet {} returned invalid JSON: {}", context, e.getMessage());
            return null;
        }
    }

    // APIs

    public Integer getHeight() {
        String response = LiteWalletJni.execute("height", "");
        JSONObject json = parseJsonObject(response, "height");
        if (json != null && json.has("height")) {
            return json.getInt("height");
        }
        return null;
    }

    public Integer getChainTip() {
        String response = LiteWalletJni.execute("info", "");
        JSONObject json = parseJsonObject(response, "info");
        if (json != null && json.has("latest_block_height")) {
            return json.getInt("latest_block_height");
        }
        return null;
    }

    public boolean isNullSeedWallet() {
        return this.isNullSeedWallet;
    }

    public Boolean isEncrypted() {
        String response = LiteWalletJni.execute("encryptionstatus", "");
        JSONObject json = parseJsonObject(response, "encryptionstatus");
        if (json != null && json.has("encrypted")) {
            return json.getBoolean("encrypted");
        }
        return null;
    }

    public boolean doEncrypt(String key) {
        String response = LiteWalletJni.execute("encrypt", key);
        JSONObject json = parseJsonObject(response, "encrypt");
        if (json != null && json.has("result")) {
            String result = json.getString("result");
            return Objects.equals(result, "success");
        }
        return false;
    }

    public boolean doDecrypt(String key) {
        String response = LiteWalletJni.execute("decrypt", key);
        JSONObject json = parseJsonObject(response, "decrypt");
        if (json != null && json.has("result")) {
            String result = json.getString("result");
            return Objects.equals(result, "success");
        }
        return false;
    }

    public boolean doUnlock(String key) {
        String response = LiteWalletJni.execute("unlock", key);
        JSONObject json = parseJsonObject(response, "unlock");
        if (json != null && json.has("result")) {
            String result = json.getString("result");
            return Objects.equals(result, "success");
        }
        return false;
    }

    public String getWalletAddress() {
        // Get balance, which also contains wallet addresses
        String response = LiteWalletJni.execute("balance", "");
        JSONObject json = parseJsonObject(response, "balance");
        String address = null;

        if (json != null && json.has("z_addresses")) {
            JSONArray z_addresses = json.getJSONArray("z_addresses");

            if (z_addresses != null && !z_addresses.isEmpty()) {
                JSONObject firstAddress = z_addresses.getJSONObject(0);
                if (firstAddress.has("address")) {
                    address = firstAddress.getString("address");
                }
            }
        }
        return address;
    }

    public String getPrivateKey() {
        String response = LiteWalletJni.execute("export", "");
        JSONArray addressesJson = parseJsonArray(response, "export");
        if (addressesJson != null && !addressesJson.isEmpty()) {
            JSONObject addressJson = addressesJson.getJSONObject(0);
            if (addressJson.has("private_key")) {
                // String address = addressJson.getString("address");
                String privateKey = addressJson.getString("private_key");
                // String viewingKey = addressJson.getString("viewing_key");

                return privateKey;
            }
        }
        return null;
    }

    public String getWalletSeed(String entropy58) {
        // Decode entropy to bytes
        byte[] myEntropyBytes = Base58.decode(entropy58);

        // Pirate library uses base64 encoding
        String myEntropy64 = Base64.toBase64String(myEntropyBytes);

        // Derive seed phrase from entropy bytes
        String mySeedResponse = LiteWalletJni.getseedphrasefromentropyb64(myEntropy64);
        JSONObject mySeedJson = parseJsonObject(mySeedResponse, "getseedphrasefromentropyb64");
        String mySeedPhrase = null;
        if (mySeedJson != null && mySeedJson.has("seedPhrase")) {
            mySeedPhrase = mySeedJson.getString("seedPhrase");

            return mySeedPhrase;
        }
        return null;
    }

    public PirateLightClient.Server getRandomServer() {
        PirateChain.PirateChainNet pirateChainNet = Settings.getInstance().getPirateChainNet();
        Collection<PirateLightClient.Server> servers = pirateChainNet.getServers();
        Random random = new Random();
        int index = random.nextInt(servers.size());
        return (PirateLightClient.Server) servers.toArray()[index];
    }

}
