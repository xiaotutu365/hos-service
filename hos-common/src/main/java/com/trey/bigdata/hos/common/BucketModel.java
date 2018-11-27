package com.trey.bigdata.hos.common;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@NoArgsConstructor
@Data
public class BucketModel {
    private String bucketId;

    private String bucketName;

    private String creator;

    private String detail;

    private Date createTime;

    public BucketModel(String bucketName, String creator, String detail) {
        // todo
        this.bucketId = null;
        this.bucketName = bucketName;
        this.creator = creator;
        this.detail = detail;
        this.createTime = new Date();
    }
}
