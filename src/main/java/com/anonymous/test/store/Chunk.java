package com.anonymous.test.store;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.anonymous.test.common.TrajectoryPoint;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description
 * @Date 2021/4/25 10:28
 * @Created by anonymous
 */
public class Chunk {

    private String sid;   // the sid of series that the chunk belongs to

    private String chunkId;

    private List<TrajectoryPoint> chunk;

    @JsonIgnore
    private int dataSize;

    public Chunk() {}

    public Chunk(String sid, String chunkId) {
        this.sid = sid;
        this.chunkId = chunkId;
        this.chunk = new ArrayList<>();
    }

    public Chunk(String sid) {
        this.sid = sid;
        this.chunk = new ArrayList<>();
    }

    private static ObjectMapper objectMapper = new ObjectMapper();
    public static String serialize(Chunk chunk) {
        String result = "";
        try {
            result = objectMapper.writeValueAsString(chunk);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static Chunk deserialize(String data) {

        Chunk chunk = null;
        try {
            chunk = objectMapper.readValue(data, Chunk.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return chunk;
    }

    public List<TrajectoryPoint> getChunk() {
        return chunk;
    }

    public int size() {
        return chunk.size();
    }

    public boolean add(TrajectoryPoint point) {
        return chunk.add(point);
    }

    public String getSid() {
        return sid;
    }

    public String getChunkId() {
        return chunkId;
    }

    public void setChunk(List<TrajectoryPoint> chunk) {
        this.chunk = chunk;
    }

    public void setChunkId(String chunkId) {
        this.chunkId = chunkId;
    }

    @Override
    public String toString() {
        return "{meta: " + "(sid: " + sid + ", chunkId: " + chunkId + ")  data: " +chunk + "}";
    }
}
