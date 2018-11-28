package com.trey.bigdata.hos.core.authmgr.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.UUID;

@NoArgsConstructor
@Data
public class TokenInfo {
    private String token;

    private int expireTime;

    private Date refreshTime;

    private Date createTime;

    private boolean isActive;

    private String creator;

    public TokenInfo(String creator) {
        this.token = UUID.randomUUID().toString();
        this.creator = creator;
        this.expireTime = 7;
        Date now = new Date();
        this.refreshTime = now;
        this.createTime = now;
        this.isActive = isActive;
    }
}
