package com.github.nexmark.flink.metric.latency;

import com.github.nexmark.flink.utils.NexmarkUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class SinkMeanLatencyMetric {

    private static final String FIELD_NAME_ID = "id";

    private static final String FIELD_NAME_VALUE = "value";

    @JsonProperty(value = FIELD_NAME_ID, required = true)
    private final String id;

    @JsonProperty(FIELD_NAME_VALUE)
    private final Double value;

    @JsonCreator
    public SinkMeanLatencyMetric(
            final @JsonProperty(value = FIELD_NAME_ID, required = true) String id,
            final @Nullable @JsonProperty(FIELD_NAME_VALUE) Double value) {

        this.id = requireNonNull(id, "id must not be null");
        this.value = value;
    }

    @JsonIgnore
    public String getId() {
        return id;
    }

    @JsonIgnore
    public Double getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SinkMeanLatencyMetric that = (SinkMeanLatencyMetric) o;
        return id.equals(that.id) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, value);
    }

    @Override
    public String toString() {
        return "SinkLatencyMetric{" +
                "id='" + id + '\'' +
                ", value=" + value +
                '}';
    }

    public static SinkMeanLatencyMetric fromJson(String json) {
        try {
            JsonNode jsonNode = NexmarkUtils.MAPPER.readTree(json);
            return NexmarkUtils.MAPPER.convertValue(jsonNode.get(0), SinkMeanLatencyMetric.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
