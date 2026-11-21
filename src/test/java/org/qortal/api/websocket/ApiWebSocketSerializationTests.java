package org.qortal.api.websocket;

import org.junit.Test;
import org.eclipse.persistence.jaxb.JAXBContextFactory;
import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.qortal.arbitrary.misc.Service;
import org.qortal.data.arbitrary.ArbitraryResourceData;
import javax.xml.bind.Marshaller;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.Assert.*;

public class ApiWebSocketSerializationTests {
    @Test
    public void chatPayloadsMatchFreshBindingMetadata() throws Exception {
        byte[] payload = "message with unicode: café".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        for (org.qortal.data.chat.ChatMessage.Encoding encoding : org.qortal.data.chat.ChatMessage.Encoding.values()) {
            Object[] messages = {
                new org.qortal.data.chat.ChatMessage(123L, 7, new byte[64], new byte[32], "sender",
                        "name", null, null, null, encoding, payload, true, false, new byte[64]),
                new org.qortal.data.chat.ActiveChats(List.of(new org.qortal.data.chat.ActiveChats.GroupChat(
                        7, "group", 123L, "sender", "name", new byte[64], encoding, payload)), List.of())
            };
            for (Object message : messages) {
                Marshaller baseline = JAXBContextFactory.createContext(new Class[]{message.getClass()}, null).createMarshaller();
                baseline.setProperty(MarshallerProperties.MEDIA_TYPE, "application/json");
                baseline.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, false);
                StringWriter expected = new StringWriter(), actual = new StringWriter();
                baseline.marshal(message, expected);
                ApiWebSocket.marshall(actual, message);
                assertEquals(expected.toString(), actual.toString());
            }
        }
    }

    @Test
    public void concurrentMessagesMatchFreshContextSerialization() throws Exception {
        var context = JAXBContextFactory.createContext(new Class[]{ArbitraryResourceData.class}, null);
        Marshaller baseline = context.createMarshaller();
        baseline.setProperty(MarshallerProperties.MEDIA_TYPE, "application/json");
        baseline.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, false);
        ExecutorService executor = Executors.newFixedThreadPool(16);
        try {
            List<Future<?>> calls = new ArrayList<>();
            for (int i = 0; i < 300; ++i) {
                final boolean list = i % 2 == 0;
                ArbitraryResourceData data = new ArbitraryResourceData(Service.DOCUMENT, "Name" + i, "identifier" + i);
                data.created = (long) i;
                // Generate the reference output serially, before concurrent marshalling.
                StringWriter reference = new StringWriter();
                baseline.marshal(list ? List.of(data, data) : data, reference);
                String expected = reference.toString();
                calls.add(executor.submit(() -> {
                    StringWriter result = new StringWriter();
                    if (list) ApiWebSocket.marshall(result, List.of(data, data));
                    else ApiWebSocket.marshall(result, data);
                    assertEquals(expected, result.toString());
                    return null;
                }));
            }
            for (Future<?> call : calls) call.get(10, TimeUnit.SECONDS);
            StringWriter empty = new StringWriter();
            ApiWebSocket.marshall(empty, Collections.emptyList());
            assertEquals("[]", empty.toString());
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
