package vkexport;

import java.nio.charset.Charset;
import java.util.Objects;

/**
 * Hilfsklasse zur Konvertierung von DOS-Strings (wie in Clipper-DBFs verwendet)
 * in Integer-Recordnummern.
 * <p>
 * Die Implementierung basiert auf den Clipper-Prozeduren:
 * <ul>
 *     <li><b>convertRecnoToInteger(char)</b>: verarbeitet 2- oder 3-stellige Zeichenketten</li>
 *     <li><b>convert2RecnoToInteger(char)</b>: verarbeitet nur 2-stellige Zeichenketten</li>
 * </ul>
 *
 * <h3>Encoding-Hinweis</h3>
 * Clipper/ASC() liefert den Bytewert gemäß DOS-Codepage (0–255).
 * In Java müssen Strings daher in das gleiche Charset konvertiert werden,
 * typischerweise <code>Cp850</code> (DACH) oder <code>IBM437</code> (US-DOS).
 *
 * <h3>Beispiel</h3>
 * <pre>{@code
 * Integer value1 = RecnoCodec.convertRecnoToInteger("ABC");        // 3 Zeichen
 * Integer value2 = RecnoCodec.convertRecnoToInteger("AB");         // 2 Zeichen
 * Integer value3 = RecnoCodec.convert2RecnoToInteger("AB");        // nur 2 Zeichen
 * }</pre>
 */
public final class RecnoCodec {

    // Typische DOS Codepage, ggf. anpassen
    private static final Charset DEFAULT_DOS_CHARSET = Charset.forName("Cp850");

    private RecnoCodec() {}

    public static Integer convertRecnoToInteger(final String s) {
        return convertRecnoToInteger(s, DEFAULT_DOS_CHARSET);
    }

    public static Integer convertRecnoToInteger(final String s, final Charset dosCharset) {
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
     * Exakte Umsetzung von PROCEDURE convert2RecnoToInteger(char)
     * Erwartet DOS-Zeichenkette mit 2 Bytes.
     */
    public static Integer convert2RecnoToInteger(final String s) {
        return convert2RecnoToInteger(s, DEFAULT_DOS_CHARSET);
    }

    public static Integer convert2RecnoToInteger(final String s, final Charset dosCharset) {
        if (s == null || s.isEmpty()) {
            return null;
        }

        Objects.requireNonNull(dosCharset, "dosCharset");
        final byte[] bytes = s.getBytes(dosCharset);

        if (bytes.length != 2) {
            System.err.println("WARNING: convert2RecnoToInteger requires exactly 2 DOS bytes (got " + bytes.length + ")");
            return  (bytes[0] & 0xFF) - 32;
        }

        final int ret1 = (bytes[0] & 0xFF) - 32; // left(char,1)
        final int ret2 = (bytes[1] & 0xFF) - 32; // substr(char,2,1)

        return ret1 * 222 + ret2;
    }

}
