package com.trey.bigdata.hos.core.usermgr.service;

import com.trey.bigdata.hos.core.usermgr.dao.UserInfoMapper;
import com.trey.bigdata.hos.core.usermgr.model.UserInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Transactional
@Service
public class UserService {

    @Autowired
    UserInfoMapper userInfoMapper;

    public boolean addUser(UserInfo userInfo) {
        userInfoMapper.addUser(userInfo);
        // todo add token
        return true;
    }

    public boolean updateUserInfo(UserInfo userInfo) {
        return true;
    }

    public boolean deleteUser(String userId) {
        userInfoMapper.deleteUser(userId);
        return true;
    }

    public UserInfo getUserInfo(String userId) {
        return userInfoMapper.getUserInfo(userId);
    }

    public UserInfo getUserInfoByName(String userName) {
        return userInfoMapper.getUserInfoByName(userName);
    }


}
