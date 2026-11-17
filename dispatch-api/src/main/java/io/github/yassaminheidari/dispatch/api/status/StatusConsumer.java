package io.github.yassaminheidari.dispatch.api.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.yassaminheidari.dispatch.api.live.JobUpdatePublisher;
import io.github.yassaminheidari.dispatch.common.JobDispatchEvent;
import io.github.yassaminheidari.dispatch.common.JobStatusEvent;
import io.github.yassaminheidari.dispatch.common.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class StatusConsumer {

    private static final Logger log = LoggerFactory.getLogger(StatusConsumer.class);

    private final StatusService statusService;
    private final JobUpdatePublisher publisher;
    private final ObjectMapper mapper;

    public StatusConsumer(StatusService statusService, JobUpdatePublisher publisher, ObjectMapper mapper) {
        this.statusService = statusService;
        this.publisher = publisher;
        this.mapper = mapper;
    }

    @KafkaListener(topics = Topics.STATUS, groupId = "dispatch-api-status")
    public void onStatus(String message) throws Exception {
        JobStatusEvent event = mapper.readValue(message, JobStatusEvent.class);
        statusService.apply(event).ifPresentOrElse(
                publisher::publish,
                () -> log.debug("Skipped duplicate or stale status event {} for job {}", event.eventId(), event.jobId()));
    }

    /** Jobs that ended in the dead-letter topic are marked FAILED with the last error message. */
    @KafkaListener(topics = Topics.DISPATCH_DLT, groupId = "dispatch-api-dlt")
    public void onDeadLetter(ConsumerRecord<String, String> record) throws Exception {
        JobDispatchEvent event = mapper.readValue(record.value(), JobDispatchEvent.class);
        Header error = record.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE);
        String detail = "Dead-lettered: " + (error == null ? "unknown error"
                : new String(error.value(), StandardCharsets.UTF_8));
        statusService.fail(event.jobId(), detail).ifPresent(publisher::publish);
    }
}
