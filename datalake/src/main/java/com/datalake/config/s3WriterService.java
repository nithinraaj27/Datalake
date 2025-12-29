package com.datalake.config;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import java.util.*;
import java.util.concurrent.*;

@Slf4j
@Service

public class s3WriterService {

    private static final int MAX_EVENTS = 5;
    private static final int FLUSH_INTERVAL_SEC = 20;

    private final S3Client s3Client;

    @Value("${aws.s3.bucket}")
    private String bucket;

    private final Map<String, List<String>> buffer  = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public s3WriterService(S3Client s3Client)
    {
        this.s3Client = s3Client;
    }

    @PostConstruct
    public void verifyAwsAccess() {
        try {
            s3Client.headBucket(b -> b.bucket(bucket));
            log.info("AWS access OK for bucket {}", bucket);
        } catch (Exception e) {
            log.error("No access to bucket {}", bucket, e);
        }
    }

    @PostConstruct
    public void schedulerFlush(){
        scheduler.scheduleAtFixedRate(
                this::flushAll,
                FLUSH_INTERVAL_SEC,
                FLUSH_INTERVAL_SEC,
                TimeUnit.SECONDS
        );
    }

    public void write(String topic, String json)
    {
        buffer.computeIfAbsent(
                topic,
                k -> Collections.synchronizedList(new ArrayList<>())
        ).add(json);

        if(buffer.get(topic).size() >= MAX_EVENTS)
        {
            flush(topic);
        }
    }

    private synchronized void flush(String topic)
    {
        List<String> events = buffer.remove(topic);
        if(events == null || events.isEmpty())
        {
            return ;
        }

        String prefix = s3PathResolver.resolve(topic, events.get(0));

        String key = prefix + topic + "_" + Instant.now().toEpochMilli() + ".json";

        String payload = String.join("\n", events);

        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType("application/json")
                .build();

        s3Client.putObject(
                request,
                software.amazon.awssdk.core.sync.RequestBody
                        .fromBytes(payload.getBytes(StandardCharsets.UTF_8))
        );

        log.info("Flushed {} events to s3: //{}{}", events.size(), bucket, key);
    }

    private void flushAll() {
        buffer.keySet().forEach(this::flush);
    }

}
