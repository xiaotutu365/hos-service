package com.trey.bigdata.hos.web;

import com.trey.bigdata.hos.common.service.HosStore;
import com.trey.bigdata.hos.core.CoreUtils;
import com.trey.bigdata.hos.core.usermgr.model.SystemRole;
import com.trey.bigdata.hos.core.usermgr.model.UserInfo;
import com.trey.bigdata.hos.core.usermgr.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

public class ApplicationInitialization implements ApplicationRunner {

    @Autowired
    private UserService userService;

    @Autowired
    private HosStore hosStore;

    @Override
    public void run(ApplicationArguments applicationArguments) throws Exception {
        UserInfo userInfo = userService.getUserInfoByName(CoreUtils.SYSTEM_USER);
        if (userInfo == null) {
            userInfo = new UserInfo(CoreUtils.SYSTEM_USER, "superadmin", "this is a superadmin", SystemRole.SUPERADMIN);
            userService.addUser(userInfo);
        }

        hosStore.createSeqTable();
    }
}
