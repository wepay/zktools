package com.wepay.zktools.test.util;

import com.wepay.zktools.util.Logging;
import org.slf4j.Logger;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Utils {
    private static final Logger logger = Logging.getLogger(Utils.class);

    @SafeVarargs
    public static <T> List<T> list(T... elem) {
        return Arrays.asList(elem);
    }

    @SafeVarargs
    public static <T> Set<T> set(T... elem) {
        return new HashSet<T>(Arrays.asList(elem));
    }

    public static void removeDirectory(File file) {
        if (file != null) {
            if (file.isDirectory()) {
                File[] list = file.listFiles();
                if (list != null) {
                    for (File f : list) {
                        Utils.removeDirectory(f);
                    }
                }
            }

            if (!file.delete()) {
                logger.warn("Cannot delete file");
            }
        }
    }

}
