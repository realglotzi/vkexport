package vkexport;

import com.linuxense.javadbf.DBFField;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static vkexport.DdlBuilder.DDL_OVERRIDES;

public class Converter {

    // Explicit per-table overrides (FIELD -> conversion); keys uppercase
    public static final Map<String, Map<String, VkExportFull.Conv>> TABLE_FIELD_OVERRIDES = new HashMap<>();
    static {
        final Map<String, VkExportFull.Conv> VGEB = new HashMap<>();
        VGEB.put("X_VAMT", VkExportFull.Conv.CONV);
        VGEB.put("G_EVK",  VkExportFull.Conv.CONV);
        VGEB.put("G_KIND", VkExportFull.Conv.CONV);
        VGEB.put("G_V_KD", VkExportFull.Conv.CONV);
        VGEB.put("G_M_KD", VkExportFull.Conv.CONV);
        VGEB.put("G_H_ORT", VkExportFull.Conv.CONV);
        for (char c = '0'; c <= '9'; c++) VGEB.put(("X_PERS"+c).toUpperCase(), VkExportFull.Conv.CONV);
        for (char c = 'A'; c <= 'Z'; c++) VGEB.put(("X_PERS"+c).toUpperCase(), VkExportFull.Conv.CONV);
        VGEB.put("X_BEM", VkExportFull.Conv.NONE);
        TABLE_FIELD_OVERRIDES.put("VGEB", VGEB);

        final Map<String, VkExportFull.Conv> VHEI = new HashMap<>();
        VHEI.put("X_VAMT", VkExportFull.Conv.CONV);
        VHEI.put("H_EBG",  VkExportFull.Conv.CONV);
        VHEI.put("H_EVBG", VkExportFull.Conv.CONV);
        VHEI.put("H_EVBT", VkExportFull.Conv.CONV);
        VHEI.put("H_HEI_ORT", VkExportFull.Conv.CONV2);
        VHEI.put("H_BG",   VkExportFull.Conv.CONV);
        VHEI.put("H_BG_V", VkExportFull.Conv.CONV);
        VHEI.put("H_BG_M", VkExportFull.Conv.CONV);
        VHEI.put("H_BT",   VkExportFull.Conv.CONV);
        VHEI.put("H_BT_V", VkExportFull.Conv.CONV);
        VHEI.put("H_BT_M", VkExportFull.Conv.CONV);
        for (char c = '0'; c <= '9'; c++) VHEI.put(("X_PERS"+c).toUpperCase(), VkExportFull.Conv.CONV);
        for (char c = 'A'; c <= 'Z'; c++) VHEI.put(("X_PERS"+c).toUpperCase(), VkExportFull.Conv.CONV);
        VHEI.put("X_BEM", VkExportFull.Conv.NONE);
        TABLE_FIELD_OVERRIDES.put("VHEI", VHEI);

        final Map<String, VkExportFull.Conv> VSTE = new HashMap<>();
        VSTE.put("X_VAMT", VkExportFull.Conv.CONV);
        for (char c = '0'; c <= '9'; c++) VSTE.put(("X_PERS"+c).toUpperCase(), VkExportFull.Conv.CONV);
        for (char c = 'A'; c <= 'Z'; c++) VSTE.put(("X_PERS"+c).toUpperCase(), VkExportFull.Conv.CONV);
        VSTE.put("S_VST",    VkExportFull.Conv.CONV);
        VSTE.put("S_VST_EP", VkExportFull.Conv.CONV);
        VSTE.put("S_VST_V",  VkExportFull.Conv.CONV);
        VSTE.put("S_VST_M",  VkExportFull.Conv.CONV);
        VSTE.put("S_EVST",  VkExportFull.Conv.CONV);
        VSTE.put("S_EVVST",  VkExportFull.Conv.CONV);
        VSTE.put("X_BEM", VkExportFull.Conv.NONE);
        TABLE_FIELD_OVERRIDES.put("VSTE", VSTE);

        final Map<String, VkExportFull.Conv> VPER = new HashMap<>();
        VPER.put("P_STPERS", VkExportFull.Conv.CONV);
        VPER.put("P_VERWGR", VkExportFull.Conv.CONV);
        VPER.put("P_ELTERN", VkExportFull.Conv.CONV);
        VPER.put("P_EHE", VkExportFull.Conv.CONV);
        VPER.put("P_BRUDER", VkExportFull.Conv.CONV);
        VPER.put("P_KET1", VkExportFull.Conv.CONV);
        VPER.put("P_KETN", VkExportFull.Conv.CONV);
        VPER.put("P_APER", VkExportFull.Conv.CONV);
        VPER.put("X_ZURK", VkExportFull.Conv.CONV);
        VPER.put("P_WOHNORT", VkExportFull.Conv.CONV);
        VPER.put("P_HER_ORT", VkExportFull.Conv.CONV);
        VPER.put("X_BEM", VkExportFull.Conv.NONE);
        TABLE_FIELD_OVERRIDES.put("VPER", VPER);

        final Map<String, VkExportFull.Conv> VAMT = new HashMap<>();
        VAMT.put("X_VAMT", VkExportFull.Conv.CONV);
        VAMT.put("X_BEM", VkExportFull.Conv.NONE);
        TABLE_FIELD_OVERRIDES.put("VAMT", VAMT);

        final Map<String, VkExportFull.Conv> VFAM = new HashMap<>();
        VFAM.put("F_KIND",   VkExportFull.Conv.CONV);
        VFAM.put("F_VATER",  VkExportFull.Conv.CONV);
        VFAM.put("F_MUTTER", VkExportFull.Conv.CONV);
        VFAM.put("F_MFG1", VkExportFull.Conv.CONV);
        VFAM.put("F_MFGN", VkExportFull.Conv.CONV);
        VFAM.put("F_VATWEH1", VkExportFull.Conv.CONV);
        VFAM.put("F_VATWEHE", VkExportFull.Conv.CONV);
        VFAM.put("F_MUTWEH1", VkExportFull.Conv.CONV);
        VFAM.put("F_MUTWEHE", VkExportFull.Conv.CONV);
        VFAM.put("F_KET1", VkExportFull.Conv.CONV);
        VFAM.put("F_KETN", VkExportFull.Conv.CONV);
        VFAM.put("F_VMEH1", VkExportFull.Conv.CONV);
        VFAM.put("F_VMEHN", VkExportFull.Conv.CONV);
        VFAM.put("F_MMEH1", VkExportFull.Conv.CONV);
        VFAM.put("F_MMEHN", VkExportFull.Conv.CONV);
        VFAM.put("X_BEM",    VkExportFull.Conv.NONE);
        TABLE_FIELD_OVERRIDES.put("VFAM", VFAM);

        final Map<String, VkExportFull.Conv> VORT = new HashMap<>();
        VORT.put("X_ORT", VkExportFull.Conv.CONV);
        VORT.put("X_BEM", VkExportFull.Conv.NONE);
        TABLE_FIELD_OVERRIDES.put("VORT", VORT);

        final Map<String, VkExportFull.Conv> VVWG = new HashMap<>();
        VVWG.put("VVG_ORT", VkExportFull.Conv.CONV);
        VVWG.put("VVG_PER", VkExportFull.Conv.CONV);
        VVWG.put("X_BEM",   VkExportFull.Conv.NONE);
        TABLE_FIELD_OVERRIDES.put("VVWG", VVWG);
    }

    // Patterns for encoded 2–3 char keys (heuristics)
    private static final List<Pattern> ENCODED_KEY_PATTERNS = Arrays.asList(
            Pattern.compile(".*_VAMT$"),
            Pattern.compile(".*_EVK$"),
            Pattern.compile(".*_KIND$"),
            Pattern.compile(".*_KD$"),
            Pattern.compile(".*_ORT$"),
            Pattern.compile("^X_PERS.*$"),
            Pattern.compile("^H_(EVBT|EBG|EVBG)$"),
            Pattern.compile("^H_HEI_ORT$"),
            Pattern.compile("^H_BG(_V|_M)?$"),
            Pattern.compile("^H_BT(_V|_M)?$"),
            Pattern.compile("^S_VST(_EP|_V|_M)?$"),
            Pattern.compile("^F_VATER$"),
            Pattern.compile("^F_MUTTER$")
    );
    private static final Set<String> ENCODED_TWOCHAR_ONLY =
            new HashSet<>(Arrays.asList("H_HEI_ORT"));


    public static VkExportFull.Conv shouldConvert(final String table, final DBFField f) {
        final String name = f.getName().toUpperCase(Locale.ROOT);
        final Map<String, VkExportFull.Conv> ov = TABLE_FIELD_OVERRIDES.getOrDefault(table, Collections.emptyMap());
        if (ov.containsKey(name)) return ov.get(name);

        final char t = f.getType().getCharCode();
        if (t=='C' && (f.getLength()==2 || f.getLength()==3)) {
            for (final Pattern p: ENCODED_KEY_PATTERNS) {
                if (p.matcher(name).matches()) {
                    return ENCODED_TWOCHAR_ONLY.contains(name) ? VkExportFull.Conv.CONV2 : VkExportFull.Conv.CONV;
                }
            }
        }
        return VkExportFull.Conv.NONE;
    }

    // Helper: check if PRG override marks a field as DATE
    public static boolean isOverrideDate(final String tableNameUpper, final String fieldLower) {
        final Map<String,String> ov = DDL_OVERRIDES.get(tableNameUpper);
        if (ov == null) return false;
        final String t = ov.get(fieldLower);
        return t != null && t.equalsIgnoreCase("varchar(10)");
    }

}
