package vkexport;

import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static vkexport.Converter.isOverrideDate;
import static vkexport.Converter.shouldConvert;
import static vkexport.MemoFieldHelper.resolveMemoFile;
import static vkexport.VkExportFull.CURRENT_CHARSET;
import static vkexport.VkExportFull.TABLES;

public class DdlBuilder {
    // Mapping von Feldnamen zu Referenztabellen
    private static final Map<String, String> FIELD_TO_TABLE;
    static {
        FIELD_TO_TABLE = new HashMap<>();
        FIELD_TO_TABLE.put("X_VAMT", "VAMT");
        FIELD_TO_TABLE.put("X_ORT", "VORT");
        FIELD_TO_TABLE.put("VVG_ORT", "VORT");
        FIELD_TO_TABLE.put("VVG_PER", "VPER");
        FIELD_TO_TABLE.put("F_KIND", "VPER");
        FIELD_TO_TABLE.put("F_VATER", "VPER");
        FIELD_TO_TABLE.put("F_MUTTER", "VPER");
        FIELD_TO_TABLE.put("P_VERWGR", "VVWG");
        FIELD_TO_TABLE.put("G_H_ORT", "VORT");
        FIELD_TO_TABLE.put("H_HEI_ORT", "VORT");
        FIELD_TO_TABLE.put("G_V_KD", "VPER");
        FIELD_TO_TABLE.put("G_M_KD", "VPER");
        FIELD_TO_TABLE.put("G_KIND", "VPER");
        FIELD_TO_TABLE.put("P_ELTERN", "VPER");
        FIELD_TO_TABLE.put("P_EHE", "VPER");
        FIELD_TO_TABLE.put("P_BRUDER", "VPER");
        FIELD_TO_TABLE.put("P_KET1", "VPER");
        FIELD_TO_TABLE.put("P_KETN", "VPER");
        FIELD_TO_TABLE.put("H_EBG", "VPER");
        FIELD_TO_TABLE.put("H_EVBG", "VPER");
        FIELD_TO_TABLE.put("H_EIBG", "VPER");
    }

    // === DDL overrides (from createtables.sql) ===
    public static final Map<String, Map<String, String>> DDL_OVERRIDES = new HashMap<>();
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

    private static String guessRefTable(final String tableUpper, final String fieldUpper) {

        if (tableUpper.equals("VPER") && (fieldUpper.equals("X_ZURK") || fieldUpper.equals("P_APER"))) return null;

        if (FIELD_TO_TABLE.containsKey(fieldUpper)) return FIELD_TO_TABLE.get(fieldUpper);

        if (fieldUpper.endsWith("_ORT")) return "VORT";
        if (fieldUpper.startsWith("X_PERS")) return "VPER";
        if (fieldUpper.endsWith("_KD") || fieldUpper.startsWith("X_") || fieldUpper.endsWith("_PER")) return "VPER";
        if (tableUpper.equals("VSTE") && (fieldUpper.startsWith("S_VST") || fieldUpper.startsWith("X_PERS"))) return "VPER";
        if (tableUpper.equals("VHEI") && (fieldUpper.startsWith("H_BG") || fieldUpper.startsWith("H_BT"))) return "VPER";
        return null;
    }

    private static List<String> emitForeignKeysForTable(final String schema, final String tableUpper, final DBFReader reader) {
        final List<String> alters = new ArrayList<>();
        final int fieldCount = reader.getFieldCount();
        for (int i = 0; i < fieldCount; i++) {
            final DBFField f = reader.getField(i);
            final String fnameU = f.getName().toUpperCase(Locale.ROOT);
            final VkExportFull.Conv c = shouldConvert(tableUpper, f);
            if (c == VkExportFull.Conv.CONV || c == VkExportFull.Conv.CONV2) {
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

    // === DDL using overrides, base-222 and DATE overrides ===
    private static String emitCreateTable(final String schema, final String tableName, final DBFReader reader) {
        final StringBuilder sb = new StringBuilder();
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
            final VkExportFull.Conv convFlag = shouldConvert(tkey, f);
            if (convFlag == VkExportFull.Conv.CONV || convFlag == VkExportFull.Conv.CONV2) {
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

    public static void build(final String pfad, final String prefix, final String schema, final String ddlOut, final String codepage) throws Exception {
        final List<String> DDL_COLLECT = new ArrayList<>();
        final List<String> FK_ALTERS = new ArrayList<>();

        for (final String t : TABLES) {
            final String dbfPath = pfad + prefix + "_" + t + ".DBF";
            final File dbfFile = new File(dbfPath);
            final File memoFile = resolveMemoFile(dbfFile);

            try (final InputStream is = new FileInputStream(dbfFile)) {
                final DBFReader reader = new DBFReader(is, CURRENT_CHARSET);
                if (memoFile != null) reader.setMemoFile(memoFile);
                reader.setCharactersetName(codepage);
//                sb.append("-- -- ").append(t).append(" -- --\n");
                FK_ALTERS.addAll(emitForeignKeysForTable(schema, t.toUpperCase(Locale.ROOT), reader));
                DDL_COLLECT.add(emitCreateTable(schema, t, reader));
            }
            try (final Writer dw = new OutputStreamWriter(new FileOutputStream(ddlOut), java.nio.charset.StandardCharsets.UTF_8)) {
                for (final String d : DDL_COLLECT) dw.write(d + "\n");
                for (final String a : FK_ALTERS) dw.write(a + "\n");
            }

        }


    }

}
