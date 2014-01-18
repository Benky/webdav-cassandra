package cz.benky.webdav.util;

import org.apache.commons.lang.StringUtils;

public final class PathUtils {

    private PathUtils() {
    }

    public static String removeTrailingSlash(String s) {
        return StringUtils.chomp(s, "/");
    }

    public static String removeFirstSlash(String s) {
        if (s == null) {
            return null;
        }
        if (s.startsWith("/")) {
            return s.substring(1);
        } else {
            return s;
        }
    }

    public static String getParentDirectory(final String path) {
        final int i = path.lastIndexOf("/");
        return path.substring(0, i);
    }

    public static String getFileName(final String path) {
        return path.replaceFirst(removeTrailingSlash(getParentDirectory(path)) + "/", "");
    }
}
