package cz.benky.webdav.util;

public final class ArrayUtils {
    private ArrayUtils() {
    }

    public static String get(final String[] directories, int index) {
        if (directories != null && directories.length > index) {
            return directories[index];
        } else {
            return null;
        }
    }
}
