package com.trey.bigdata.hos.core.authmgr.service;

import com.trey.bigdata.hos.core.authmgr.model.ServiceAuth;
import com.trey.bigdata.hos.core.authmgr.model.TokenInfo;

public class AuthService {
    public void addAuth(ServiceAuth serviceAuth) {

    }

    public void deleteAuth(String bucketName,String token) {

    }

    public void deleteAuthByToken(String token) {

    }

    public void deleteAuthByBucket(String bucket) {

    }

    public ServiceAuth getServiceAuth(String bucket, String token) {

    }

    public TokenInfo getTokenInfo(String token) {

    }

    public boolean addToken(TokenInfo tokenInfo) {

    }

    public boolean deleteToken(String token) {

    }

    public boolean updateToken(String token, int expireTime, boolean isActive) {

    }

    public boolean refreshToken(String token) {

    }

    public boolean checkToken(String token) {

    }
}