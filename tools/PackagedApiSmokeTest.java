import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.qortal.api.ApiService;
import org.qortal.settings.Settings;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Security;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/** Exercises the distributed JAR, independent of Maven/IDE output directories. */
public class PackagedApiSmokeTest {
    public static void main(String[] args) {
        Thread watchdog = new Thread(() -> {
            try {
                Thread.sleep(60000);
                System.err.println("Packaged API smoke test exceeded 60 seconds");
                System.exit(2);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }, "packaged-api-watchdog");
        watchdog.setDaemon(true);
        watchdog.start();

        try {
            rejectCompilerErrorStubs(Path.of(args[0]));
            Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
            Files.writeString(Path.of("settings.json"),
                    "{\"apiPort\":0,\"bindAddress\":\"127.0.0.1\","
                    + "\"apiWhitelist\":[\"127.0.0.1\"],\"apiDocumentationEnabled\":true,"
                    + "\"autoUpdateEnabled\":false,\"uPnPEnabled\":false}");
            Settings.fileInstance("settings.json");

            ApiService api = ApiService.getInstance();
            try {
                api.start();
                assertApiResponses(api);
                api.restart();
                assertApiResponses(api);
            } finally {
                api.stop();
            }
            System.out.println("PASS: packaged API starts, serves HTTP/OpenAPI, restarts and stops");
            System.exit(0);
        } catch (Throwable failure) {
            failure.printStackTrace();
            System.exit(1);
        }
    }

    private static void rejectCompilerErrorStubs(Path artifact) throws Exception {
        List<String> brokenClasses = new ArrayList<>();
        try (JarFile jar = new JarFile(artifact.toFile())) {
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                if (!entry.getName().startsWith("org/qortal/") || !entry.getName().endsWith(".class"))
                    continue;
                try (java.io.InputStream input = jar.getInputStream(entry)) {
                    String bytecode = new String(input.readAllBytes(), StandardCharsets.ISO_8859_1);
                    if (bytecode.contains("Unresolved compilation problem"))
                        brokenClasses.add(entry.getName());
                }
            }
        }
        if (!brokenClasses.isEmpty())
            throw new IllegalStateException("JAR contains compiler error stubs: " + brokenClasses);
        System.out.println("PASS: no Qortal compiler error stubs in the JAR");
    }

    private static void assertApiResponses(ApiService api) throws Exception {
        Field serverField = ApiService.class.getDeclaredField("server");
        serverField.setAccessible(true);
        Server server = (Server) serverField.get(api);
        int port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        assertResponse(client, port, "/admin/dbpool", "[]");
        assertResponse(client, port, "/crosschain/arrr/validateaddress?address=invalid", "false");
        assertResponse(client, port, "/api-documentation/", null);
        assertResponse(client, port, "/openapi.json", "\"openapi\"");
    }

    private static void assertResponse(HttpClient client, int port, String path, String expected) throws Exception {
        URI uri = URI.create("http://127.0.0.1:" + port + path);
        HttpRequest request = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(20)).GET().build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200 || (expected != null && !response.body().contains(expected)))
            throw new IllegalStateException(path + " returned HTTP " + response.statusCode()
                    + ": " + response.body().substring(0, Math.min(300, response.body().length())));
        System.out.println("PASS: HTTP 200 " + path);
    }
}
