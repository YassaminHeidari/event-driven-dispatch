package io.github.yassaminheidari.dispatch.api.live;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Pushes every job update to all connected browsers on this instance (endpoint: /ws/jobs). */
@Component
public class JobUpdatesWebSocketHandler extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(JobUpdatesWebSocketHandler.class);

    private final Map<String, WebSocketSession> sessions = new ConcurrentHashMap<>();

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        // WebSocketSession is not thread-safe. The decorator serialises sends and drops slow clients.
        sessions.put(session.getId(), new ConcurrentWebSocketSessionDecorator(session, 5_000, 512 * 1024));
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessions.remove(session.getId());
    }

    public void broadcast(String json) {
        TextMessage message = new TextMessage(json);
        sessions.values().forEach(session -> {
            try {
                if (session.isOpen()) {
                    session.sendMessage(message);
                }
            } catch (Exception e) {
                log.debug("Dropping WebSocket session {}: {}", session.getId(), e.toString());
                sessions.remove(session.getId());
            }
        });
    }

    public int connectedClients() {
        return sessions.size();
    }
}
