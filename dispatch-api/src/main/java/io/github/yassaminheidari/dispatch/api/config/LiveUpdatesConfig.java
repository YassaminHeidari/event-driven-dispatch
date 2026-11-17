package io.github.yassaminheidari.dispatch.api.config;

import io.github.yassaminheidari.dispatch.api.live.JobUpdatePublisher;
import io.github.yassaminheidari.dispatch.api.live.JobUpdatesWebSocketHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

import java.nio.charset.StandardCharsets;

@Configuration
@EnableWebSocket
public class LiveUpdatesConfig implements WebSocketConfigurer {

    private final JobUpdatesWebSocketHandler handler;

    public LiveUpdatesConfig(JobUpdatesWebSocketHandler handler) {
        this.handler = handler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(handler, "/ws/jobs").setAllowedOriginPatterns("*");
    }

    /** Every API instance subscribes to the Redis channel and forwards messages to its WebSocket clients. */
    @Bean
    RedisMessageListenerContainer jobUpdatesListener(RedisConnectionFactory connectionFactory) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(
                (message, pattern) -> handler.broadcast(new String(message.getBody(), StandardCharsets.UTF_8)),
                new ChannelTopic(JobUpdatePublisher.CHANNEL));
        return container;
    }
}
