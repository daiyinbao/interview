package com.dyb.asyncscheduler.util;

import java.time.Instant;

public final class DebugLog {
    private static final String PROP = "async.debug";
    private static final boolean ENABLED = Boolean.parseBoolean(System.getProperty(PROP, "true"));

    private DebugLog() {}

    public static boolean enabled() {
        return ENABLED;
    }

    public static void log(String message) {
        if (!ENABLED) return;
        System.out.println(prefix() + message);
    }

    public static void log(String format, Object... args) {
        if (!ENABLED) return;
        System.out.println(prefix() + String.format(format, args));
    }

    private static String prefix() {
        return Instant.now() + " [" + Thread.currentThread().getName() + "] ";
    }
}
