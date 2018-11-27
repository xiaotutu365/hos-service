package com.trey.bigdata.hos.common;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class HosObjectSummary implements Comparable<HosObjectSummary>, Serializable {
    private String id;

    private String key;

    private String name;

    private long length;

    private String mediaType;

    private long lastModifyTime;

    private String bucket;

    private Map<String, String> attrs;

    public String getContentEncoding() {
        return attrs != null ? attrs.get("content-encoding") : null;
    }

    @Override
    public int compareTo(HosObjectSummary objectSummary) {
        return this.getKey().compareTo(objectSummary.getKey());
    }
}