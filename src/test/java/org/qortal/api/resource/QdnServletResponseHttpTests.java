package org.qortal.api.resource;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.qortal.api.gateway.resource.GatewayResource;
import org.qortal.api.domainmap.resource.DomainMapResource;
import org.qortal.api.restricted.resource.RenderResource;
import org.qortal.arbitrary.ArbitraryDataFile.ResourceIdType;
import org.qortal.arbitrary.ArbitraryDataReader;
import org.qortal.arbitrary.ArbitraryDataResource;
import org.qortal.arbitrary.misc.Service;
import org.qortal.controller.arbitrary.ArbitraryDataRenderManager;
import org.qortal.test.common.Common;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;

import static org.junit.Assert.*;

/** Real Jetty/Jersey dispatch must retain directly streamed bytes and their status. */
public class QdnServletResponseHttpTests {
    private Server server;
    private String base;
    private Path cache;
    private String hash;
    private final HttpClient client = HttpClient.newHttpClient();

    @Before public void fixture() throws Exception {
        Common.useDefaultSettings();
        hash = UUID.randomUUID().toString();
        ArbitraryDataReader reader = new ArbitraryDataReader(hash, ResourceIdType.FILE_HASH, Service.ARBITRARY_DATA, null);
        cache = reader.getUncompressedPath();
        Files.createDirectories(cache);
        Files.writeString(cache.resolve("index.html"), "<html><head></head><body>qdn-wire-fixture</body></html>");
        Files.write(cache.resolve("asset.bin"), new byte[]{0, 1, 2, (byte) 255});
        ArbitraryDataRenderManager.getInstance().addToAuthorizedResources(new ArbitraryDataResource(hash, null, null, null));
    }

    private void start(Class<?> resource) throws Exception {
        if (resource == DomainMapResource.class) {
            var domainMap = org.qortal.settings.Settings.class.getDeclaredField("domainMap");
            domainMap.setAccessible(true);
            domainMap.set(org.qortal.settings.Settings.getInstance(), java.util.Collections.emptyList());
        }
        server = new Server(new InetSocketAddress("127.0.0.1", 0));
        ServletContextHandler handler = new ServletContextHandler();
        handler.setContextPath("/");
        handler.addServlet(new ServletHolder(new ServletContainer(new ResourceConfig().register(resource))), "/*");
        server.setHandler(handler);
        server.start();
        base = "http://127.0.0.1:" + ((ServerConnector) server.getConnectors()[0]).getLocalPort();
    }

    private HttpResponse<byte[]> get(String path) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(10)).build(),
                HttpResponse.BodyHandlers.ofByteArray());
    }

    @Test public void gatewayEmptyRootRetains200() throws Exception {
        start(GatewayResource.class);
        HttpResponse<byte[]> result = get("/");
        assertEquals(200, result.statusCode());
        assertEquals(0, result.body().length);
    }

    @Test public void domainMapMissingMappingRetains404Body() throws Exception {
        start(DomainMapResource.class);
        HttpResponse<byte[]> result = get("/missing");
        assertEquals(404, result.statusCode());
        assertEquals("Error 404: File Not Found", new String(result.body(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test public void smallHtmlRetains200AndInjectedHeaders() throws Exception {
        start(RenderResource.class);
        HttpResponse<byte[]> result = get("/render/hash/" + hash);
        assertEquals(200, result.statusCode());
        String html = new String(result.body(), java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(html, html.contains("qdn-wire-fixture"));
        assertTrue(result.headers().firstValue("Content-Security-Policy").isPresent());
        assertEquals(result.body().length, Long.parseLong(result.headers().firstValue("Content-Length").orElseThrow()));
    }

    @Test public void binaryAssetHasExactLengthAndBytes() throws Exception {
        start(RenderResource.class);
        HttpResponse<byte[]> result = get("/render/hash/" + hash + "/asset.bin");
        assertEquals(200, result.statusCode());
        assertArrayEquals(new byte[]{0, 1, 2, (byte) 255}, result.body());
        assertEquals("4", result.headers().firstValue("Content-Length").orElseThrow());
    }

    @Test public void missingCachedAssetRetains404() throws Exception {
        start(RenderResource.class);
        HttpResponse<byte[]> result = get("/render/hash/" + hash + "/missing.bin");
        assertEquals(404, result.statusCode());
        assertEquals("Error 404: File Not Found", new String(result.body(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @After public void close() throws Exception {
        if (server != null) { server.stop(); server.join(); }
        if (cache != null) org.apache.commons.io.FileUtils.deleteDirectory(cache.getParent().toFile());
    }
}
