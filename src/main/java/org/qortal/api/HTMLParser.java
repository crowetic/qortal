package org.qortal.api;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.qortal.arbitrary.misc.Service;
import java.util.Objects;
public class HTMLParser {

    private static final Logger LOGGER = LogManager.getLogger(HTMLParser.class);

    private String qdnBase;
    private String qdnBaseWithPath;
    private byte[] data;
    private String qdnContext;
    private String resourceId;
    private Service service;
    private String identifier;
    private String path;
    private String theme;
    private String lang;
    private boolean usingCustomRouting;

    public HTMLParser(String resourceId, String inPath, String prefix, boolean includeResourceIdInPrefix, byte[] data,
                      String qdnContext, Service service, String identifier, String theme, boolean usingCustomRouting,  String lang) {
        String inPathWithoutFilename = inPath.contains("/") ? inPath.substring(0, inPath.lastIndexOf('/')) : String.format("/%s",inPath);
        this.qdnBase = includeResourceIdInPrefix ? String.format("%s/%s", prefix, resourceId) : prefix;
        this.qdnBaseWithPath = includeResourceIdInPrefix ? String.format("%s/%s%s", prefix, resourceId, inPathWithoutFilename) : String.format("%s%s", prefix, inPathWithoutFilename);
        this.data = data;
        this.qdnContext = qdnContext;
        this.resourceId = resourceId;
        this.service = service;
        this.identifier = identifier;
        this.path = inPath;
        this.theme = theme;
        this.lang = lang;
        this.usingCustomRouting = usingCustomRouting;
    }

    public void addAdditionalHeaderTags() {
        String fileContents = new String(data);
        Document document = Jsoup.parse(fileContents);
        Elements head = document.getElementsByTag("head");
        if (!head.isEmpty()) {
            // Add q-apps script tag
            String qAppsScriptElement = String.format("<script src=\"/apps/q-apps.js?time=%d\">", System.currentTimeMillis());
            head.get(0).prepend(qAppsScriptElement);

            // Add q-apps gateway script tag if in gateway mode
            if (Objects.equals(this.qdnContext, "gateway")) {
                String qAppsGatewayScriptElement = String.format("<script src=\"/apps/q-apps-gateway.js?time=%d\">", System.currentTimeMillis());
                head.get(0).prepend(qAppsGatewayScriptElement);
            }

            // Escape and add vars
            String qdnContext = this.qdnContext != null ? this.qdnContext.replace("\\", "").replace("\"","\\\"") : "";
            String service = this.service.toString().replace("\\", "").replace("\"","\\\"");
            String name = this.resourceId != null ? this.resourceId.replace("\\", "").replace("\"","\\\"") : "";
            String identifier = this.identifier != null ? this.identifier.replace("\\", "").replace("\"","\\\"") : "";
            String path = this.path != null ? this.path.replace("\\", "").replace("\"","\\\"") : "";
            String theme = this.theme != null ? this.theme.replace("\\", "").replace("\"","\\\"") : "";
            String lang = this.lang != null ? this.lang.replace("\\", "").replace("\"", "\\\"") : "";
            String qdnBase = this.qdnBase != null ? this.qdnBase.replace("\\", "").replace("\"","\\\"") : "";
            String qdnBaseWithPath = this.qdnBaseWithPath != null ? this.qdnBaseWithPath.replace("\\", "").replace("\"","\\\"") : "";
            String qdnContextVar = String.format(
                "<script>var _qdnContext=\"%s\"; var _qdnTheme=\"%s\"; var _qdnLang=\"%s\"; var _qdnService=\"%s\"; var _qdnName=\"%s\"; var _qdnIdentifier=\"%s\"; var _qdnPath=\"%s\"; var _qdnBase=\"%s\"; var _qdnBaseWithPath=\"%s\";</script>",
                qdnContext, theme, lang, service, name, identifier, path, qdnBase, qdnBaseWithPath
              );
            head.get(0).prepend(qdnContextVar);

            // Add base href tag
            // Exclude the path if this request was routed back to the index automatically
            String baseHref = this.usingCustomRouting ? this.qdnBase : this.qdnBaseWithPath;
            String baseElement = String.format("<base href=\"%s/\">", baseHref);
            head.get(0).prepend(baseElement);

            // Add meta charset tag
            String metaCharsetElement = "<meta charset=\"UTF-8\">";
            head.get(0).prepend(metaCharsetElement);

        }
        String html = document.html();
        this.data = html.getBytes();
    }

    public static boolean isHtmlFile(String path) {
        if (path.endsWith(".html") || path.endsWith(".htm") || path.isEmpty()) {
            return true;
        }
        return false;
    }

    public byte[] getData() {
        return this.data;
    }
}
