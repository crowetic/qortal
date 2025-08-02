package org.qortal.api.websocket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketException;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.qortal.data.crosschain.UnsignedFeeEvent;
import org.qortal.event.Event;
import org.qortal.event.EventBus;
import org.qortal.event.FeeWaitingEvent;
import org.qortal.event.Listener;

import java.io.IOException;
import java.io.StringWriter;

@WebSocket
@SuppressWarnings("serial")
public class UnsignedFeesSocket extends ApiWebSocket implements Listener {

	private static final Logger LOGGER = LogManager.getLogger(UnsignedFeesSocket.class);

	@Override
	public void configure(WebSocketServletFactory factory) {
		factory.register(UnsignedFeesSocket.class);

		EventBus.INSTANCE.addListener(this);
	}

	@Override
	public void listen(Event event) {
		if (!(event instanceof FeeWaitingEvent))
			return;

		for (Session session : getSessions()) {
			FeeWaitingEvent feeWaitingEvent = (FeeWaitingEvent) event;
			sendUnsignedFeeEvent(session, new UnsignedFeeEvent(feeWaitingEvent.isPositive(), feeWaitingEvent.getAddress()));
		}
	}


	@OnWebSocketConnect
	@Override
	public void onWebSocketConnect(Session session) {
		super.onWebSocketConnect(session);
	}

	@OnWebSocketClose
	@Override
	public void onWebSocketClose(Session session, int statusCode, String reason) {
		super.onWebSocketClose(session, statusCode, reason);
	}

	@OnWebSocketError
	public void onWebSocketError(Session session, Throwable throwable) {
		/* We ignore errors for now, but method here to silence log spam */
	}

	@OnWebSocketMessage
	public void onWebSocketMessage(Session session, String message) {
	}

	private void sendUnsignedFeeEvent(Session session, UnsignedFeeEvent unsignedFeeEvent) {
		StringWriter stringWriter = new StringWriter();

		try {
			marshall(stringWriter, unsignedFeeEvent);

			session.getRemote().sendStringByFuture(stringWriter.toString());
		} catch (IOException | WebSocketException e) {
			// No output this time
		}
	}

}