package com.trey.bigdata.hos.core;

import java.util.UUID;

public class CoreUtils {
    public final static String SYSTEM_USER = "SuperAdmin";

    public static String getUUIDStr() {
        return UUID.randomUUID().toString().replace("-", "");
    }
}
