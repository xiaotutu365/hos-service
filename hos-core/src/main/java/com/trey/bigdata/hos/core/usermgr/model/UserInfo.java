package com.trey.bigdata.hos.core.usermgr.model;

import com.trey.bigdata.hos.core.CoreUtils;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@NoArgsConstructor
@Data
public class UserInfo {
    private String userId;

    private String userName;

    private String password;

    private String detail;

    private SystemRole systemRole;

    private Date createTime;

    public UserInfo(String userName, String password, String detail, SystemRole systemRole, Date createTime) {
        this.userId = CoreUtils.getUUIDStr();
        this.userName = userName;
        // todo
        this.password = password;
        this.detail = detail;
        this.systemRole = systemRole;
        this.createTime = createTime;
    }

}