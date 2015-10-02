/*
 * Copyright 2015 Axibase Corporation or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * https://www.axibase.com/atsd/axibase-apache-2.0.pdf
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.axibase.collector;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.regex.Pattern;

public class Utils {
    public static final String DEFAULT_ENTITY = "defaultEntity";
    private static final Pattern SPACE = Pattern.compile("[[\\s]]");
    private static final Pattern QUOTES = Pattern.compile("[[\'|\"]]");
    private static final Pattern D_QUOTE = Pattern.compile("[[\"]]");
    public static final Charset UTF_8 = Charset.forName("UTF-8");

    public static String sanitizeEntity(String s) {
        return sanitize(s);
    }

    public static String sanitizeMetric(String s) {
        return sanitize(s);
    }

    public static String sanitizeTagKey(String s) {
        return sanitize(s);
    }

    public static String sanitizeTagValue(String s) {
        s = D_QUOTE.matcher(s).replaceAll("\\\\\"");
        if (s.contains(" ") || s.contains("=")) {
            StringBuilder sb = new StringBuilder("\"");
            s = sb.append(s).append("\"").toString();
        }
        return s;
    }

    public static String sanitizeMessage(String s) {
        s = D_QUOTE.matcher(s).replaceAll("\\\\\"");
        if (s.contains(" ")) {
            StringBuilder sb = new StringBuilder("\"");
            s = sb.append(s).append("\"").toString();
        }
        return s;
    }

    public static String sanitize(String s) {
        s = SPACE.matcher(s).replaceAll("_");
        s = QUOTES.matcher(s).replaceAll("");
        return s;
    }

    public static String resolveHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return DEFAULT_ENTITY;
        }
    }
}