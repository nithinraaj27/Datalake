package com.datalake.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class s3PathResolver {

    private static final ObjectMapper mapper = new ObjectMapper();

    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);

    private static final DateTimeFormatter HOUR = DateTimeFormatter.ofPattern("HH").withZone(ZoneOffset.UTC);

    public static String resolve(String topic, String json){
        try{
            JsonNode node = mapper.readTree(json);
            String eventTime = node.get("event_time").asText();

            Instant instant = Instant.parse(eventTime);

            return String.format(
                    "topic=%s/date=%s/hour=%s/",
                    topic,
                    DATE.format(instant),
                    HOUR.format(instant)
            );

        }catch(Exception e){
            return String.format("topic=%s/date=unknown/hour=unknown/", topic);
        }
    }

}
