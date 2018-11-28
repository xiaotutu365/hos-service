package com.trey.bigdata.hos.core.authmgr.model;

import lombok.Data;

import java.util.Date;

@Data
public class ServiceAuth {
    private String bucketName;

    private String targetToken;

    private Date authTime;
}
