package vkexport;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * VkExportFull — DBF → SQL exporter
 * - Global .DBT memo pointer logic for ALL 'M' fields
 * - Base-222 key conversion (Clipper) with overrides + heuristics
 * - DDL declares Base-222 fields as INT4 (FK/IDs), not VARCHAR(2|3)
 * - Uses DBFField.getType().getCharCode() for type checks
 */
public class VkExportFull {

    enum Conv {NONE, CONV, CONV2}

    public static final List<String> TABLES =
            Arrays.asList("VGEB", "VHEI", "VSTE", "VPER", "VAMT", "VFAM", "VORT", "VVWG");

    public static Charset CURRENT_CHARSET = Charset.forName("Cp850");

    // Low-level DBF layout parser (offsets and sizes)
    public static class DbfLayout {
        final int headerLen;
        final int recordLen;
        final Map<String, Integer> fieldOffset = new HashMap<>();
        final Map<String, Integer> fieldLength = new HashMap<>();

        DbfLayout(final int h, final int r) {
            headerLen = h;
            recordLen = r;
        }
    }

    // === main ===
    public static void main(final String[] args) throws Exception {
        final Map<String, String> cli = parseArgs(args);
        final String pfad = require(cli, "--pfad");
        final String prefix = require(cli, "--prefix");
        final String schema = require(cli, "--schema");
        final String sqlInsertFile = cli.getOrDefault("--out", "export.sql");
        final String ddlOut = cli.get("--ddl-out");
        final String codepage = cli.getOrDefault("--cp", "Cp850");
        final int batchSize = Integer.parseInt(cli.getOrDefault("--batch-size", "1"));

        CURRENT_CHARSET = Charset.forName(codepage);

        if (ddlOut != null) {
            DdlBuilder.build(pfad, prefix, schema, ddlOut, codepage);
        }

        InsertBuilder.build(pfad, prefix, schema, batchSize, codepage, sqlInsertFile);
    }


    private static Map<String, String> parseArgs(final String[] args) {
        final Map<String, String> m = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                m.put(args[i], (i + 1 < args.length && !args[i + 1].startsWith("--")) ? args[++i] : "true");
            }
        }
        return m;
    }

    private static String require(final Map<String, String> m, final String key) {
        if (!m.containsKey(key)) throw new IllegalArgumentException("Missing arg: " + key);
        return m.get(key);
    }

}
