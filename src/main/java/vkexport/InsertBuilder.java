package vkexport;

import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFRow;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import static vkexport.Converter.isOverrideDate;
import static vkexport.Converter.shouldConvert;
import static vkexport.MemoFieldHelper.readMemoViaPointer;
import static vkexport.MemoFieldHelper.resolveMemoFile;
import static vkexport.VkExportFull.CURRENT_CHARSET;
import static vkexport.VkExportFull.TABLES;

public class InsertBuilder {

    public static void build(final String pfad, final String prefix, final String schema, final int batchSize, final String codepage, final String sqlInsertFile) throws  Exception {
        final StringBuilder sb = new StringBuilder();

        for (final String t : TABLES) {
            final String dbfPath = pfad + prefix + "_" + t + ".DBF";
            final File dbfFile = new File(dbfPath);
            final File memoFile = resolveMemoFile(dbfFile);
            final String dbtPath = memoFile != null ? memoFile.getAbsolutePath() : null;

            final VkExportFull.DbfLayout layout = parseDbfLayout(dbfFile);

            try (final InputStream is2 = new FileInputStream(dbfFile)) {
                final DBFReader reader2 = new DBFReader(is2, CURRENT_CHARSET);
                if (memoFile != null) reader2.setMemoFile(memoFile);
                reader2.setCharactersetName(codepage);
                DBFRow row;
                int recno = 0;
                int __batchCount = 0; boolean __batchOpen = false;
                while ((row = reader2.nextRow()) != null) {
                    recno++;
                    final String __ins = emitInsert(schema, t, dbfFile, dbtPath, layout, reader2, row, recno);
                    if (batchSize > 1) {
                        if (!__batchOpen) {
                            final String __prefix = extractInsertPrefix(__ins);
                            if (__prefix == null) { sb.append(__ins).append("\n"); continue; }
                            sb.append(__prefix);
                            __batchOpen = true; __batchCount = 0;
                        }
                        final String __tuple = extractTupleFromInsert(__ins);
                        if (__tuple == null) { sb.append(__ins).append("\n"); continue; }
                        sb.append(__batchCount == 0 ? "  " : ", ").append(__tuple).append("\n");
                        __batchCount++;
                        if (__batchCount >= batchSize) { sb.append(";\n"); __batchOpen = false; __batchCount = 0; }
                    } else {
                        sb.append(__ins).append("\n");
                    }
                }
                if (__batchOpen) { sb.append(";\n"); __batchOpen = false; __batchCount = 0; }
                sb.append("\n");
            }
        }

        try (final Writer w = new OutputStreamWriter(new FileOutputStream(sqlInsertFile), java.nio.charset.StandardCharsets.UTF_8)) {
            w.write(sb.toString());
        }

    }

    // === INSERT builder with memo-pointer, base-222 and DATE conversion per override ===
    private static String emitInsert(final String schema, final String tableName, final File dbfFile, final String dbtPath, final VkExportFull.DbfLayout layout,
                                     final DBFReader reader, final DBFRow row, final int recno) throws IOException {
        final List<String> cols = new ArrayList<>();
        cols.add("x_recno");
        final int fieldCount = reader.getFieldCount();
        for (int i = 0; i < fieldCount; i++) cols.add(reader.getField(i).getName().toLowerCase(Locale.ROOT));

        final List<String> vals = new ArrayList<>();
        vals.add(String.valueOf(recno));
        for (int i = 0; i < fieldCount; i++) {
            final DBFField f = reader.getField(i);
            final char t = f.getType().getCharCode();
            final String fname = f.getName();
            final String fnameUpper = fname.toUpperCase(Locale.ROOT);

            Object v;
            if (t=='M') {
                v = readMemoViaPointer(dbfFile, dbtPath, layout, recno, fnameUpper, CURRENT_CHARSET);
            } else if (t=='L') {
                final Boolean b = row.getBoolean(fname);
                vals.add(b == null ? "NULL" : (b ? "TRUE" : "FALSE"));
                continue;
            } else if (t=='D') {
                final Date d = row.getDate(fname);
                v = (d == null) ? null : new SimpleDateFormat("yyyyMMdd").format(d);
            } else if (t=='T') {
                final Date d = row.getDate(fname);
                v = (d == null) ? null : new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(d);
            } else if (t=='G' || t=='P' || t=='Q') {
                final byte[] bytes = (byte[]) row.getObject(i);
                vals.add(bytesToPgHex(bytes));
                continue;
            } else {
                v = row.getObject(i);
            }

            // X_STAND: DATE via TO_DATE(YYYYMMDD)
            if ("x_stand".equals(fname.toLowerCase(Locale.ROOT))) {
                String s = asString(v);
                if (s != null) s = s.trim();
                final String tt = (s==null) ? null : s.replaceAll("[^0-9]", "");
                if (tt != null && tt.matches("\\d{8}")) {
                    vals.add("TO_DATE('" + tt + "', 'YYYYMMDD')");
                } else if (tt != null && tt.matches("\\d{4}")) {
                    vals.add("TO_DATE('" + tt + "0101', 'YYYYMMDD')");
                } else {
                    vals.add("NULL");
                }
                continue;
            }
            // Some known DATE fields that often contain "  .  ." or "  ."
            if ("x_udatfr".equals(fname.toLowerCase(Locale.ROOT))) {
                if (asString(v).startsWith("  .  .")) {
                    System.err.println("DBG: " + tableName + "." + fname + " recno=" + recno + " date looks like '" + asString(v) + "'");
                    vals.add("NULL");
                    continue;
                }
            }
            if ("g_geb_dfr".equals(fname.toLowerCase(Locale.ROOT))) {
                if (asString(v).startsWith("  .  .")) {
                    System.err.println("DBG: " + tableName + "." + fname + " recno=" + recno + " date looks like '" + asString(v) + "'");
                    vals.add("NULL");
                    continue;
                }
            }
            if ("g_geb_std".equals(fname.toLowerCase(Locale.ROOT))) {
                if (asString(v).startsWith("  .")) {
                    System.err.println("DBG: " + tableName + "." + fname + " recno=" + recno + " date looks like '" + asString(v) + "'");
                    vals.add("NULL");
                    continue;
                }
            }

            // DATE override from PRG?
            if (isOverrideDate(tableName.toUpperCase(Locale.ROOT), fname.toLowerCase(Locale.ROOT))) {
                String s = asString(v);
                if (s != null) s = s.trim();

                if (s != null && s.matches("\\d{8}")) {
                    vals.add("'" + s.substring(0,2) + "." + s.substring(2,4) + "." + s.substring(4,8) + "'");
                } else if (s != null && s.matches("\\d{6}")) {
                    vals.add("'" + String.format("%10s", s.substring(0,2) + "." + s.substring(2,6)) + "'");
                } else if (s != null && s.matches("\\d{4}")) {
                    vals.add("'" + String.format("%10s", s) + "'");
                } else if (s == null || s.isEmpty()) {
                    vals.add("NULL");
                } else {
                    // fallback: try ISO date literal
                    vals.add(escSql(s));
                }
                continue;
            }
            // Base-222 conversion for keys
            final VkExportFull.Conv which = shouldConvert(tableName.toUpperCase(Locale.ROOT), f);
            if (which == VkExportFull.Conv.CONV)  v = RecnoCodec.convertRecnoToLong(asString(v));
            if (which == VkExportFull.Conv.CONV2) v = RecnoCodec.convert2RecnoToLong(asString(v));

            vals.add(escSql(v));
        }
        return "INSERT INTO " + schema + "." + tableName.toLowerCase(Locale.ROOT) +
                " (" + String.join(", ", cols) + ") VALUES (" + String.join(", ", vals) + ");";
    }

    private static String asString(final Object v) { return v == null ? null : v.toString(); }

    private static String bytesToPgHex(final byte[] data) {
        if (data == null) return "NULL";
        final StringBuilder hex = new StringBuilder();
        hex.append("'\\x");
        for (final byte b : data) hex.append(String.format("%02x", b));
        hex.append("'");
        return hex.toString();
    }

    private static String escSql(final Object v) {
        if (v == null) return "NULL";
        if (v instanceof Number) return v.toString();
        if (v instanceof Boolean) return ((Boolean)v) ? "TRUE" : "FALSE";
        return "'" + v.toString().replace("'", "''") + "'";
    }

    private static VkExportFull.DbfLayout parseDbfLayout(final File dbfFile) throws IOException {
        try (final RandomAccessFile raf = new RandomAccessFile(dbfFile, "r")) {
            final byte[] header = new byte[32];
            raf.readFully(header);
            final int headerLen = (header[8]&0xFF) | ((header[9]&0xFF)<<8);
            final int recordLen = (header[10]&0xFF) | ((header[11]&0xFF)<<8);
            final VkExportFull.DbfLayout layout = new VkExportFull.DbfLayout(headerLen, recordLen);
            int offset = 0;
            while (true) {
                final byte[] desc = new byte[32];
                raf.readFully(desc);
                if (desc[0] == 0x0D) break;
                final String name = new String(desc, 0, 11, java.nio.charset.StandardCharsets.US_ASCII)
                        .split("\u0000",2)[0].trim().toUpperCase(Locale.ROOT);
                final int len = desc[16] & 0xFF;
                layout.fieldOffset.put(name, offset);
                layout.fieldLength.put(name, len);
                offset += len;
            }
            return layout;
        }
    }

    // --- Helpers for batching existing single-row INSERT SQL (no changes to value/memo/date logic) ---
    private static String extractInsertPrefix(final String insertSql) {
        final int idx = insertSql.toUpperCase(java.util.Locale.ROOT).indexOf("VALUES");
        if (idx <= 0) return null;
        return insertSql.substring(0, idx).trim() + " VALUES \n";
    }
    private static String extractTupleFromInsert(final String insertSql) {
        final int idx = insertSql.toUpperCase(java.util.Locale.ROOT).indexOf("VALUES");
        if (idx < 0) return null;
        final int open = insertSql.indexOf('(', idx);
        final int close = insertSql.lastIndexOf(')');
        if (open < 0 || close < 0 || close <= open) return null;
        return insertSql.substring(open, close+1);
    }


}
