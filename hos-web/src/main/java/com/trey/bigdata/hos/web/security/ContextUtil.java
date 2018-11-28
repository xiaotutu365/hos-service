package com.trey.bigdata.hos.web.security;

import com.trey.bigdata.hos.core.usermgr.model.UserInfo;

public class ContextUtil {

    public final static String SESSION_KEY = "USER_TOKEN";

    private static ThreadLocal<UserInfo> userInfoThreadLocal = new ThreadLocal<>();

    public static UserInfo getCurrentUser() {
        return userInfoThreadLocal.get();
    }

    public static void setCurrentUser(UserInfo userInfo) {
        userInfoThreadLocal.set(userInfo);
    }

    public static void clear() {
        userInfoThreadLocal.remove();
    }
}
