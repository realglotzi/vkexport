package vkexport;

import java.nio.charset.Charset;
import java.util.Objects;

public final class RecnoCodec {

    // Typische DOS Codepage, ggf. anpassen
    private static final Charset DEFAULT_DOS_CHARSET = Charset.forName("Cp850");

    private RecnoCodec() {}

    public static Integer convertRecnoToLong(final String s) {
        return convertRecnoToLong(s, DEFAULT_DOS_CHARSET);
    }

    public static Integer convertRecnoToLong(final String s, final Charset dosCharset) {
        if (s == null || s.isEmpty()) {
            return null;
        }

        Objects.requireNonNull(dosCharset, "dosCharset");

        final byte[] bytes = s.getBytes(dosCharset);

        if (bytes.length == 3) {
            final int ret1 = (bytes[0] & 0xFF) - 32;
            final int ret2 = (bytes[1] & 0xFF) - 32;
            final int ret3 = (bytes[2] & 0xFF) - 32;

            return (ret1 * 222 + ret2) * 222 + ret3;
        } else if (bytes.length == 2) {
            final int ret1 = (bytes[0] & 0xFF) - 32;
            final int ret2 = (bytes[1] & 0xFF) - 32;

            return ret1 * 222 + ret2;
        } else if (bytes.length == 1) {
            System.err.println("WARNING: length must be 2 or 3 DOS bytes (got " + bytes.length + ")");
            return (bytes[0] & 0xFF) - 32;
        }

        return null;
    }

    /**
     * Exakte Umsetzung von PROCEDURE convert2RecnoToLong(char)
     * Erwartet DOS-Zeichenkette mit 2 Bytes.
     */
    public static Integer convert2RecnoToLong(final String s) {
        return convert2RecnoToLong(s, DEFAULT_DOS_CHARSET);
    }

    public static Integer convert2RecnoToLong(final String s, final Charset dosCharset) {
        if (s == null || s.isEmpty()) {
            return null;
        }

        Objects.requireNonNull(dosCharset, "dosCharset");
        final byte[] bytes = s.getBytes(dosCharset);

        if (bytes.length != 2) {
            System.err.println("WARNING: convert2RecnoToLong requires exactly 2 DOS bytes (got " + bytes.length + ")");
            return  (bytes[0] & 0xFF) - 32;
        }

        final int ret1 = (bytes[0] & 0xFF) - 32; // left(char,1)
        final int ret2 = (bytes[1] & 0xFF) - 32; // substr(char,2,1)

        return ret1 * 222 + ret2;
    }

}
