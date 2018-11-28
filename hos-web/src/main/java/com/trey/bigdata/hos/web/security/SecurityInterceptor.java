package com.trey.bigdata.hos.web.security;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.trey.bigdata.hos.core.authmgr.model.TokenInfo;
import com.trey.bigdata.hos.core.authmgr.service.AuthService;
import com.trey.bigdata.hos.core.usermgr.model.SystemRole;
import com.trey.bigdata.hos.core.usermgr.model.UserInfo;
import com.trey.bigdata.hos.core.usermgr.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.util.concurrent.TimeUnit;

public class SecurityInterceptor implements HandlerInterceptor {

    @Autowired
    private AuthService authService;

    @Autowired
    private UserService userService;

    private Cache<String, UserInfo> userInfoCache = CacheBuilder.newBuilder().expireAfterWrite(20, TimeUnit.MINUTES).build();


    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object object) throws Exception {
        // 判断是否是登录页面，如果是登录页面，则直接通过
        if (request.getRequestURI().equals("/loginPost")) {
            return true;
        }
        String token;
        HttpSession session = request.getSession();
        if (session.getAttribute(ContextUtil.SESSION_KEY) != null) {
            token = session.getAttribute(ContextUtil.SESSION_KEY).toString();
        } else {
            token = request.getHeader("X-Auth-Token");
        }
        TokenInfo tokenInfo = authService.getTokenInfo(token);
        if (tokenInfo == null) {
            String url = "/loginPost";
            response.sendRedirect(url);
            return false;
        }
        UserInfo userInfo = userInfoCache.getIfPresent(tokenInfo.getToken());
        if (userInfo == null) {
            userInfo = userService.getUserInfo(token);
            userInfo.setSystemRole(SystemRole.VISITOR);
            userInfo.setUserName("Visitor");
            userInfo.setDetail("this is a visitor");
            userInfo.setUserId(token);
        }
        userInfoCache.put(tokenInfo.getToken(), userInfo);
        ContextUtil.setCurrentUser(userInfo);
        return true;
    }

    @Override
    public void postHandle(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, Object o, ModelAndView modelAndView) throws Exception {

    }

    @Override
    public void afterCompletion(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, Object o, Exception e) throws Exception {

    }
}
