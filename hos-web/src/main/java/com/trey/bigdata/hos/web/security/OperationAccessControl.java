package com.trey.bigdata.hos.web.security;

import com.trey.bigdata.hos.common.BucketModel;
import com.trey.bigdata.hos.common.service.BucketService;
import com.trey.bigdata.hos.core.CoreUtils;
import com.trey.bigdata.hos.core.authmgr.model.ServiceAuth;
import com.trey.bigdata.hos.core.authmgr.model.TokenInfo;
import com.trey.bigdata.hos.core.authmgr.service.AuthService;
import com.trey.bigdata.hos.core.usermgr.model.SystemRole;
import com.trey.bigdata.hos.core.usermgr.model.UserInfo;
import com.trey.bigdata.hos.core.usermgr.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;

public class OperationAccessControl {

    @Autowired
    private AuthService authService;

    @Autowired
    private UserService userService;

    @Autowired
    private BucketService bucketService;

    UserInfo checkLogin(String userName, String password) {
        UserInfo userInfo = userService.getUserInfoByName(userName);
        if (userInfo == null) {
            return null;
        }
        return userInfo.getPassword().equals(password) ? userInfo : null;
    }

    public boolean checkSystemRole(SystemRole systemRole1, SystemRole systemRole2) {
        if (systemRole1.equals(SystemRole.SUPERADMIN)) {
            return true;
        }
        return systemRole1.equals(SystemRole.ADMIN) && systemRole2.equals(SystemRole.USER);
    }

    public boolean checkSystemRole(SystemRole systemRole, String userId) {
        if (systemRole.equals(SystemRole.SUPERADMIN)) {
            return true;
        }
        UserInfo userInfo = userService.getUserInfo(userId);
        return systemRole.equals(SystemRole.ADMIN) && userInfo.getSystemRole().equals(SystemRole.USER);
    }

    public boolean checkTokenOwner(String userName, String token) {
        TokenInfo tokenInfo = authService.getTokenInfo(token);
        return tokenInfo.getCreator().equals(userName);
    }

    public boolean checkBucketOwner(String userName, String bucket) {
        BucketModel bucketModel = bucketService.getBucketByName(bucket);
        return bucketModel.getCreator().equals(userName);
    }

    public boolean checkPermission(String token, String bucket) {
        if (authService.checkToken(token)) {
            ServiceAuth serviceAuth = authService.getServiceAuth(bucket, token);
            if (serviceAuth != null) {
                return true;
            }
        }
        return false;
    }
}
