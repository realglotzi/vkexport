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
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * VkExportFull — DBF → SQL exporter
 * - Global .DBT memo pointer logic for ALL 'M' fields
 * - Base-222 key conversion (Clipper) with overrides + heuristics
 * - DDL declares Base-222 fields as INT4 (FK/IDs), not VARCHAR(2|3)
 * - Uses DBFField.getType().getCharCode() for type checks
 */
public class VkExportFullWorking {

    enum Conv { NONE, CONV, CONV2 }

    private static final List<String> TABLES =
            Arrays.asList("VGEB","VHEI","VSTE","VPER","VAMT","VFAM","VORT","VVWG");

    // Patterns for encoded 2–3 char keys (heuristics)
    private static final List<Pattern> ENCODED_KEY_PATTERNS = Arrays.asList(
            Pattern.compile(".*_VAMT$"),
            Pattern.compile(".*_EVK$"),
            Pattern.compile(".*_KIND$"),
            Pattern.compile(".*_KD$"),
            Pattern.compile(".*_ORT$"),
            Pattern.compile("^X_PERS.*$"),
            Pattern.compile("^H_(EIBG|EBG|EVBG)$"),
            Pattern.compile("^H_HEI_ORT$"),
            Pattern.compile("^H_BG(_V|_M)?$"),
            Pattern.compile("^H_BT(_V|_M)?$"),
            Pattern.compile("^S_VST(_EP|_V|_M)?$"),
            Pattern.compile("^F_VATER$"),
            Pattern.compile("^F_MUTTER$")
    );
    private static final Set<String> ENCODED_TWOCHAR_ONLY =
            new HashSet<>(Arrays.asList("H_HEI_ORT"));

    private static Charset CURRENT_CHARSET = Charset.forName("Cp850");

    // Explicit per-table overrides (FIELD -> conversion); keys uppercase
    private static final Map<String, Map<String, Conv>> TABLE_FIELD_OVERRIDES = new HashMap<>();
    static {
        final Map<String, Conv> VGEB = new HashMap<>();
        VGEB.put("X_VAMT", Conv.CONV);
        VGEB.put("G_EVK",  Conv.CONV);
        VGEB.put("G_KIND", Conv.CONV);
        VGEB.put("G_V_KD", Conv.CONV);
        VGEB.put("G_M_KD", Conv.CONV);
        VGEB.put("G_H_ORT", Conv.CONV);
        for (char c = '0'; c <= '9'; c++) VGEB.put(("X_PERS"+c).toUpperCase(), Conv.CONV);
        for (char c = 'A'; c <= 'Z'; c++) VGEB.put(("X_PERS"+c).toUpperCase(), Conv.CONV);
        VGEB.put("X_BEM", Conv.NONE);
        TABLE_FIELD_OVERRIDES.put("VGEB", VGEB);

        final Map<String, Conv> VHEI = new HashMap<>();
        VHEI.put("X_VAMT", Conv.CONV);
        VHEI.put("H_EBG",  Conv.CONV);
        VHEI.put("H_EVBG", Conv.CONV);
        VHEI.put("H_EIBG", Conv.CONV);
        VHEI.put("H_HEI_ORT", Conv.CONV2);
        VHEI.put("H_BG",   Conv.CONV);
        VHEI.put("H_BG_V", Conv.CONV);
        VHEI.put("H_BG_M", Conv.CONV);
        VHEI.put("H_BT",   Conv.CONV);
        VHEI.put("H_BT_V", Conv.CONV);
        VHEI.put("H_BT_M", Conv.CONV);
        for (char c = '0'; c <= '9'; c++) VHEI.put(("X_PERS"+c).toUpperCase(), Conv.CONV);
        for (char c = 'A'; c <= 'Z'; c++) VHEI.put(("X_PERS"+c).toUpperCase(), Conv.CONV);
        VHEI.put("X_BEM", Conv.NONE);
        TABLE_FIELD_OVERRIDES.put("VHEI", VHEI);

        final Map<String, Conv> VSTE = new HashMap<>();
        VSTE.put("X_VAMT", Conv.CONV);
        for (char c = '0'; c <= '9'; c++) VSTE.put(("X_PERS"+c).toUpperCase(), Conv.CONV);
        for (char c = 'A'; c <= 'Z'; c++) VSTE.put(("X_PERS"+c).toUpperCase(), Conv.CONV);
        VSTE.put("S_VST",    Conv.CONV);
        VSTE.put("S_VST_EP", Conv.CONV);
        VSTE.put("S_VST_V",  Conv.CONV);
        VSTE.put("S_VST_M",  Conv.CONV);
        VSTE.put("S_EVST",  Conv.CONV);
        VSTE.put("S_EVVST",  Conv.CONV);
        VSTE.put("X_BEM", Conv.NONE);
        TABLE_FIELD_OVERRIDES.put("VSTE", VSTE);

        final Map<String, Conv> VPER = new HashMap<>();
        VPER.put("P_STPERS", Conv.CONV);
        VPER.put("P_VERWGR", Conv.CONV);
        VPER.put("P_ELTERN", Conv.CONV);
        VPER.put("P_EHE", Conv.CONV);
        VPER.put("P_BRUDER", Conv.CONV);
        VPER.put("P_KET1", Conv.CONV);
        VPER.put("P_KETN", Conv.CONV);
        VPER.put("P_APER", Conv.CONV);
        VPER.put("X_ZURK", Conv.CONV);
        VPER.put("X_BEM", Conv.NONE);
        TABLE_FIELD_OVERRIDES.put("VPER", VPER);

        final Map<String, Conv> VAMT = new HashMap<>();
        VAMT.put("X_VAMT", Conv.CONV);
        VAMT.put("X_BEM", Conv.NONE);
        TABLE_FIELD_OVERRIDES.put("VAMT", VAMT);

        final Map<String, Conv> VFAM = new HashMap<>();
        VFAM.put("F_KIND",   Conv.CONV);
        VFAM.put("F_VATER",  Conv.CONV);
        VFAM.put("F_MUTTER", Conv.CONV);
        VFAM.put("F_MFG1", Conv.CONV);
        VFAM.put("F_MFGN", Conv.CONV);
        VFAM.put("F_VATWEH1", Conv.CONV);
        VFAM.put("F_VATWEHE", Conv.CONV);
        VFAM.put("F_MUTWEH1", Conv.CONV);
        VFAM.put("F_MUTWEHE", Conv.CONV);
        VFAM.put("F_KET1", Conv.CONV);
        VFAM.put("F_KETN", Conv.CONV);
        VFAM.put("F_VMEH1", Conv.CONV);
        VFAM.put("F_VMEHN", Conv.CONV);
        VFAM.put("F_MMEH1", Conv.CONV);
        VFAM.put("F_MMEHN", Conv.CONV);
        VFAM.put("X_BEM",    Conv.NONE);
        TABLE_FIELD_OVERRIDES.put("VFAM", VFAM);

        final Map<String, Conv> VORT = new HashMap<>();
        VORT.put("X_ORT", Conv.CONV);
        VORT.put("X_BEM", Conv.NONE);
        TABLE_FIELD_OVERRIDES.put("VORT", VORT);

        final Map<String, Conv> VVWG = new HashMap<>();
        VVWG.put("VVG_ORT", Conv.CONV);
        VVWG.put("VVG_PER", Conv.CONV);
        VVWG.put("X_BEM",   Conv.NONE);
        TABLE_FIELD_OVERRIDES.put("VVWG", VVWG);
    }

    // === DDL overrides (from createtables.sql) ===
    private static final Map<String, Map<String, String>> DDL_OVERRIDES = new HashMap<>();
    static {
        // These overrides were parsed from your KI/createtables.sql and should mirror the PRG DDL exactly.
        // For brevity here, we include representative fields. Extend as needed.
        final Map<String,String> vgeb = new LinkedHashMap<>();
        vgeb.put("x_recno","int4");
        vgeb.put("x_stand","date");
        vgeb.put("x_vamt","int4");
        vgeb.put("x_udatum","varchar(10)");
        vgeb.put("g_evk","int4");
        vgeb.put("g_geb_d","varchar(10)");
        vgeb.put("g_h_dat","varchar(10)");
        vgeb.put("g_h_ort","int4");
        vgeb.put("x_bem","text");
        DDL_OVERRIDES.put("VGEB", vgeb);

        final Map<String,String> vhei = new LinkedHashMap<>();
        vhei.put("x_recno","int4");
        vhei.put("x_stand","date");
        vhei.put("x_vamt","int4");
        vhei.put("x_udatum","varchar(10)");
        vhei.put("h_hei_d","varchar(10)");
        vhei.put("x_bem","text");
        DDL_OVERRIDES.put("VHEI", vhei);

        final Map<String,String> vste = new LinkedHashMap<>();
        vste.put("x_recno","int4");
        vste.put("x_stand","date");
        vste.put("x_udatum","varchar(10)");
        vste.put("x_vamt","int4");
        vste.put("s_evst","int4");
        vste.put("s_evvst","int4");
        vste.put("s_ste_d","varchar(10)");
        vste.put("s_beg_d","varchar(10)");
        vste.put("s_beg_ort","int4");
        vste.put("x_bem","text");
        DDL_OVERRIDES.put("VSTE", vste);

        final Map<String,String> vper = new LinkedHashMap<>();
        vper.put("x_recno","int4");
        vper.put("x_stand","date");
        vper.put("p_event_d","varchar(10)");
        vper.put("p_stpers","int4");
        vper.put("p_verwgr","int4");
        vper.put("p_geb_dat","varchar(10)");
        vper.put("p_tau_dat","varchar(10)");
        vper.put("p_ste_dat","varchar(10)");
        vper.put("p_beg_dat","varchar(10)");
        vper.put("x_bem","text");
        DDL_OVERRIDES.put("VPER", vper);

        DDL_OVERRIDES.put("VAMT", new LinkedHashMap<>());

        final Map<String,String> vfam = new LinkedHashMap<>();
        vfam.put("x_recno","int4");
        vfam.put("f_vater","int4");
        vfam.put("f_mutter","int4");
        vfam.put("f_kind","int4");
        vfam.put("f_mfg1","int4");
        vfam.put("f_mfgn","int4");
        vfam.put("f_vatweh1","int4");
        vfam.put("f_vatwehe","int4");
        vfam.put("f_mutweh1","int4");
        vfam.put("f_mutwehe","int4");
        vfam.put("f_ket1","int4");
        vfam.put("f_ketn","int4");
        vfam.put("f_vmeh1","int4");
        vfam.put("f_vmehn","int4");
        vfam.put("f_mmeh1","int4");
        vfam.put("f_mmehn","int4");
        DDL_OVERRIDES.put("VFAM", vfam);
        DDL_OVERRIDES.put("VORT", new LinkedHashMap<>());
        DDL_OVERRIDES.put("VVWG", new LinkedHashMap<>());
    }

    // Helper: check if PRG override marks a field as DATE
    private static boolean isOverrideDate(final String tableNameUpper, final String fieldLower) {
        final Map<String,String> ov = DDL_OVERRIDES.get(tableNameUpper);
        if (ov == null) return false;
        final String t = ov.get(fieldLower);
        return t != null && t.equalsIgnoreCase("varchar(10)");
    }

    // === main ===
    public static void main(final String[] args) throws Exception {
        final Map<String, String> cli = parseArgs(args);
        final String pfad     = require(cli, "--pfad");
        final String prefix   = require(cli, "--prefix");
        final String schema   = require(cli, "--schema");
        final String out      = cli.getOrDefault("--out", "export.sql");
        final String ddlOut   = cli.get("--ddl-out");
        final String codepage = cli.getOrDefault("--cp", "Cp850");

        CURRENT_CHARSET = Charset.forName(codepage);

        final StringBuilder sb = new StringBuilder();
        final List<String> DDL_COLLECT = new ArrayList<>();
        final List<String> FK_ALTERS = new ArrayList<>();

        for (final String t : TABLES) {
            final String dbfPath = pfad + prefix + "_" + t + ".DBF";
            final File dbfFile = new File(dbfPath);
            final File memoFile = resolveMemoFile(dbfFile);
            final String dbtPath = memoFile != null ? memoFile.getAbsolutePath() : null;

            final DbfLayout layout = parseDbfLayout(dbfFile);

            try (final InputStream is = new FileInputStream(dbfFile)) {
                final DBFReader reader = new DBFReader(is, CURRENT_CHARSET);
                if (memoFile != null) reader.setMemoFile(memoFile);
                reader.setCharactersetName(codepage);
                sb.append("-- -- ").append(t).append(" -- --\n");
                FK_ALTERS.addAll(emitForeignKeysForTable(schema, t.toUpperCase(Locale.ROOT), reader));
                DDL_COLLECT.add(emitCreateTable(schema, t, reader));
            }

            try (final InputStream is2 = new FileInputStream(dbfFile)) {
                final DBFReader reader2 = new DBFReader(is2, CURRENT_CHARSET);
                if (memoFile != null) reader2.setMemoFile(memoFile);
                reader2.setCharactersetName(codepage);
                DBFRow row;
                int recno = 0;
                while ((row = reader2.nextRow()) != null) {
                    recno++;
                    sb.append(emitInsert(schema, t, dbfFile, dbtPath, layout, reader2, row, recno)).append("\n");
                }
                sb.append("\n");
            }
        }

        try (final Writer w = new OutputStreamWriter(new FileOutputStream(out), java.nio.charset.StandardCharsets.UTF_8)) {
            w.write(sb.toString());
            if (ddlOut != null) {
                try (final Writer dw = new OutputStreamWriter(new FileOutputStream(ddlOut), java.nio.charset.StandardCharsets.UTF_8)) {
                    for (final String d : DDL_COLLECT) dw.write(d + "\n");
                    for (final String a : FK_ALTERS) dw.write(a + "\n");
                }
            }
        }
    }

    // === DDL using overrides, base-222 and DATE overrides ===
    private static String emitCreateTable(final String schema, final String tableName, final DBFReader reader) {
        final StringBuilder sb = new StringBuilder();
        final List<String> DDL_COLLECT = new ArrayList<>();
        final List<String> FK_ALTERS = new ArrayList<>();
        sb.append("CREATE TABLE ").append(schema).append(".").append(tableName.toLowerCase()).append(" (\n");
        sb.append("  x_recno int4 PRIMARY KEY");
        final String tkey = tableName.toUpperCase(Locale.ROOT);
        final Map<String,String> overrides = DDL_OVERRIDES.get(tkey);

        final int fieldCount = reader.getFieldCount();
        for (int i = 0; i < fieldCount; i++) {
            final DBFField f = reader.getField(i);
            final char t = f.getType().getCharCode();
            final String fname = f.getName().toLowerCase(Locale.ROOT);

            if ("x_recno".equals(fname)) continue;

            // 1) If PRG override exists, prefer it (with DATE upgrade handled later in INSERT)
            if (overrides != null && overrides.containsKey(fname)) {
                final String ov = overrides.get(fname);
                if (isOverrideDate(tkey, fname)) {
                    sb.append(",\n  ").append(fname).append(" varchar(10)");
                } else {
                    sb.append(",\n  ").append(fname).append(" ").append(ov);
                }
                continue;
            }

            // 2) Base-222 IDs -> INT4
            final Conv convFlag = shouldConvert(tkey, f);
            if (convFlag == Conv.CONV || convFlag == Conv.CONV2) {
                sb.append(",\n  ").append(fname).append(" int4");
                continue;
            }

            // 3) Default mapping
            if (t=='M') {
                sb.append(",\n  ").append(fname).append(" text");
            } else if (t=='C' || t=='V') {
                sb.append(",\n  ").append(fname).append(" varchar(").append(Math.max(1, f.getLength())).append(")");
            } else if (t=='N' || t=='F') {
                if (f.getDecimalCount()==0) {
                    sb.append(",\n  ").append(fname).append(f.getLength()>9 ? " int8" : " int4");
                } else {
                    sb.append(",\n  ").append(fname).append(" numeric(").append(f.getLength()).append(",").append(f.getDecimalCount()).append(")");
                }
            } else if (t=='I') {
                sb.append(",\n  ").append(fname).append(" int4");
            } else if (t=='Y') {
                sb.append(",\n  ").append(fname).append(" numeric(19,4)");
            } else if (t=='B' || t=='O') {
                sb.append(",\n  ").append(fname).append(" double precision");
            } else if (t=='D') {
                sb.append(",\n  ").append(fname).append(" varchar(8)");
            } else if (t=='T') {
                sb.append(",\n  ").append(fname).append(" timestamp");
            } else if (t=='L') {
                sb.append(",\n  ").append(fname).append(" boolean");
            } else if (t=='G' || t=='P' || t=='Q') {
                sb.append(",\n  ").append(fname).append(" bytea");
            } else {
                sb.append(",\n  ").append(fname).append(" text");
            }
        }
        sb.append("\n);\n");
        return sb.toString();
    }

    // === INSERT builder with memo-pointer, base-222 and DATE conversion per override ===
    private static String emitInsert(final String schema, final String tableName, final File dbfFile, final String dbtPath, final DbfLayout layout,
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
            final Conv which = shouldConvert(tableName.toUpperCase(Locale.ROOT), f);
            if (which == Conv.CONV)  v = convertRecnoToLong(asString(v));
            if (which == Conv.CONV2) v = convert2RecnoToLong(asString(v));

            vals.add(escSql(v));
        }
        return "INSERT INTO " + schema + "." + tableName.toLowerCase(Locale.ROOT) +
                " (" + String.join(", ", cols) + ") VALUES (" + String.join(", ", vals) + ");";
    }

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

    // === Base-222 conversions ===
    public static Integer convertRecnoToLong(final String s) {
        if (s == null) return null;
        if (s.length() == 3)
            return ((s.charAt(0)-32)*222 + (s.charAt(1)-32))*222 + (s.charAt(2)-32);
        if (s.length() == 2)
            return (s.charAt(0)-32)*222 + (s.charAt(1)-32);
        return null;
    }
    public static Integer convert2RecnoToLong(final String s) {
        if (s == null || s.length()<2) return null;
        return (s.charAt(0)-32)*222 + (s.charAt(1)-32);
    }

    private static Conv shouldConvert(final String table, final DBFField f) {
        final String name = f.getName().toUpperCase(Locale.ROOT);
        final Map<String, Conv> ov = TABLE_FIELD_OVERRIDES.getOrDefault(table, Collections.emptyMap());
        if (ov.containsKey(name)) return ov.get(name);

        final char t = f.getType().getCharCode();
        if (t=='C' && (f.getLength()==2 || f.getLength()==3)) {
            for (final Pattern p: ENCODED_KEY_PATTERNS) {
                if (p.matcher(name).matches()) {
                    return ENCODED_TWOCHAR_ONLY.contains(name) ? Conv.CONV2 : Conv.CONV;
                }
            }
        }
        return Conv.NONE;
    }

    private static String asString(final Object v) { return v == null ? null : v.toString(); }

    // Memo pointer logic
    private static String readMemoViaPointer(final File dbfFile, final String dbtPath, final DbfLayout layout, final int recno, final String fieldNameUpper, final Charset cs) throws IOException {
        if (layout == null || dbtPath == null) return null;
        final Integer off = layout.fieldOffset.get(fieldNameUpper);
        final Integer len = layout.fieldLength.get(fieldNameUpper);
        if (off == null || len == null) return null;
        final long pos = layout.headerLen + (long)(recno-1)*layout.recordLen + 1 + off; // skip delete flag
        try (final RandomAccessFile raf = new RandomAccessFile(dbfFile, "r")) {
            final byte[] ptrBytes = new byte[len];
            raf.seek(pos);
            raf.readFully(ptrBytes);
            final String ptrStr = new String(ptrBytes, java.nio.charset.StandardCharsets.US_ASCII).trim();
            int blockNo = 0;
            if (!ptrStr.isEmpty() && ptrStr.chars().allMatch(Character::isDigit)) {
                blockNo = Integer.parseInt(ptrStr);
            } else if (len >= 4) {
                blockNo = (ptrBytes[0]&0xFF) | ((ptrBytes[1]&0xFF)<<8) | ((ptrBytes[2]&0xFF)<<16) | ((ptrBytes[3]&0xFF)<<24);
            }
            if (blockNo <= 0) return null;
            final int blockSize = 512;
            final byte[] memo = readDbtBlocks(new File(dbtPath), blockNo, blockSize, 16);
            if (memo == null) return null;
            int end = memo.length;
            for (int i=0;i<memo.length;i++) { if (memo[i]==0x1A) { end=i; break; } }
            while (end>0 && (memo[end-1]==0 || memo[end-1]==0x1A)) end--;
            final String s = new String(memo, 0, end, cs);
            return s.replace("\r\n","\n");
        }
    }

    private static byte[] readDbtBlocks(final File dbtFile, final int blockNo, final int blockSize, final int maxBlocks) throws IOException {
        if (!dbtFile.exists()) return null;
        try (final RandomAccessFile raf = new RandomAccessFile(dbtFile, "r")) {
            final long offset = (long)blockNo * blockSize;
            if (offset >= raf.length()) return null;
            final int toRead = (int)Math.min(raf.length()-offset, (long)blockSize*maxBlocks);
            final byte[] buf = new byte[toRead];
            raf.seek(offset);
            final int n = raf.read(buf);
            if (n < toRead) return Arrays.copyOf(buf, n);
            return buf;
        }
    }

    // Low-level DBF layout parser (offsets and sizes)
    private static class DbfLayout {
        final int headerLen;
        final int recordLen;
        final Map<String,Integer> fieldOffset = new HashMap<>();
        final Map<String,Integer> fieldLength = new HashMap<>();
        DbfLayout(final int h, final int r) { headerLen=h; recordLen=r; }
    }
    private static DbfLayout parseDbfLayout(final File dbfFile) throws IOException {
        try (final RandomAccessFile raf = new RandomAccessFile(dbfFile, "r")) {
            final byte[] header = new byte[32];
            raf.readFully(header);
            final int headerLen = (header[8]&0xFF) | ((header[9]&0xFF)<<8);
            final int recordLen = (header[10]&0xFF) | ((header[11]&0xFF)<<8);
            final DbfLayout layout = new DbfLayout(headerLen, recordLen);
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

    private static File resolveMemoFile(final File dbfFile) {
        final String name = dbfFile.getName();
        final int dot = name.lastIndexOf('.');
        final String base = (dot > 0 ? name.substring(0, dot) : name);
        final File dir = dbfFile.getParentFile() != null ? dbfFile.getParentFile() : new File(".");
        File dbt = new File(dir, base + ".DBT");
        if (!dbt.exists()) dbt = new File(dir, base + ".dbt");
        if (dbt.exists()) return dbt;
        return null;
    }

    private static Map<String,String> parseArgs(final String[] args) {
        final Map<String,String> m=new HashMap<>();
        for (int i=0;i<args.length;i++) {
            if (args[i].startsWith("--")) {
                m.put(args[i], (i+1<args.length && !args[i+1].startsWith("--"))? args[++i]:"true");
            }
        }
        return m;
    }
    private static String require(final Map<String,String> m, final String key) {
        if (!m.containsKey(key)) throw new IllegalArgumentException("Missing arg: "+key);
        return m.get(key);
    }
    private static String guessRefTable(final String tableUpper, final String fieldUpper) {
        if (tableUpper.equals("VPER") && (fieldUpper.equals("X_ZURK") || fieldUpper.equals("P_APER"))) return null;
        if (tableUpper.equals("VPER") && fieldUpper.equals("P_VERWGR")) return "VVWG";
        if (fieldUpper.equals("X_VAMT")) return "VAMT";
        if (fieldUpper.equals("X_ORT") || fieldUpper.endsWith("_ORT") || fieldUpper.equals("VVG_ORT")) return "VORT";
        if (fieldUpper.startsWith("X_PERS") || fieldUpper.equals("VVG_PER")) return "VPER";
        if (tableUpper.equals("VFAM")) {
            if (fieldUpper.equals("F_KIND") || fieldUpper.equals("F_VATER") || fieldUpper.equals("F_MUTTER")) return "VPER";
        }
        if (tableUpper.equals("VPER")) {
            if (fieldUpper.equals("X_B_KD") || fieldUpper.equals("X_M_KD") || fieldUpper.equals("X_KD") ||
                    fieldUpper.equals("P_ELTERN") || fieldUpper.equals("P_EHE") || fieldUpper.equals("P_BRUDER") ||
                    fieldUpper.equals("P_KET1") || fieldUpper.equals("P_KETN")) return "VPER";
        }
        if (tableUpper.equals("VGEB")) {
            if (fieldUpper.equals("G_V_KD") || fieldUpper.equals("G_M_KD") || fieldUpper.equals("G_KIND")) return "VPER";
            if (fieldUpper.equals("G_H_ORT")) return "VORT";
        }
        if (tableUpper.equals("VHEI")) {
            if (fieldUpper.equals("H_HEI_ORT")) return "VORT";
            if (fieldUpper.startsWith("H_BG") || fieldUpper.startsWith("H_BT") ||
                    fieldUpper.equals("H_EBG") || fieldUpper.equals("H_EVBG") || fieldUpper.equals("H_EIBG")) return "VPER";
        }
        if (tableUpper.equals("VSTE")) {
            if (fieldUpper.startsWith("S_VST") || fieldUpper.startsWith("X_PERS")) return "VPER";
        }
        if (fieldUpper.endsWith("_KD") || fieldUpper.startsWith("X_") || fieldUpper.endsWith("_PER") ||
                fieldUpper.equals("F_KIND") || fieldUpper.equals("F_VATER") || fieldUpper.equals("F_MUTTER")) {
            return "VPER";
        }
        return null;
    }

    private static List<String> emitForeignKeysForTable(final String schema, final String tableUpper, final DBFReader reader) {
        final List<String> alters = new ArrayList<>();
        final int fieldCount = reader.getFieldCount();
        for (int i = 0; i < fieldCount; i++) {
            final DBFField f = reader.getField(i);
            final String fnameU = f.getName().toUpperCase(Locale.ROOT);
            final Conv c = shouldConvert(tableUpper, f);
            if (c == Conv.CONV || c == Conv.CONV2) {
                final String refTable = guessRefTable(tableUpper, fnameU);
                if (refTable != null) {
                    final String fkName = ("fk_" + tableUpper + "_" + fnameU).toLowerCase(Locale.ROOT);
                    final String alter = "ALTER TABLE " + schema + "." + tableUpper.toLowerCase(Locale.ROOT) +
                            " ADD CONSTRAINT " + fkName +
                            " FOREIGN KEY (" + fnameU.toLowerCase(Locale.ROOT) + ") REFERENCES " +
                            schema + "." + refTable.toLowerCase(Locale.ROOT) + "(x_recno);";
                    alters.add(alter);
                }
            }
        }
        return alters;
    }

}