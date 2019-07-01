package com.wepay.zktools.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Logging {

    public static Logger getLogger(Class<?> clazz) {
        return LoggerFactory.getLogger(clazz);
    }

}
