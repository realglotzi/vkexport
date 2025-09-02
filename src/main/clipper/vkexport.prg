PROCEDURE main (prefix, pfad, schema)

  IF PCOUNT() != 3
    vkhelp()
    QUIT
  ENDIF
  
  pfad = pfad + "\"
  
  SET TALK off
  SET ALTERNATE TO export.sql
  SET ALTERNATE on
    
  SET CONSOLE OFF
  ? "DROP SCHEMA " + schema + " cascade;"
  ? "CREATE SCHEMA " + schema + ";"
  ? "SET search_path = pg_catalog, " + schema + ";"
  SET CONSOLE ON

  SET ALTERNATE OFF
  ? "Konvertiere VGEB ... "
  SET ALTERNATE ON

  SET CONSOLE OFF
  convertVGEB(prefix, pfad, schema)
  SET CONSOLE ON
  
  SET ALTERNATE OFF
  ? "Konvertiere VHEI ... "
  SET ALTERNATE ON
  SET CONSOLE OFF
  convertVHEI(prefix, pfad, schema)
  SET CONSOLE ON
  
  SET ALTERNATE OFF
  ? "Konvertiere VSTE ... "
  SET ALTERNATE ON
  SET CONSOLE OFF
  convertVSTE(prefix, pfad, schema)
  SET CONSOLE ON
  
  SET ALTERNATE OFF
  ? "Konvertiere VPER ... "
  SET ALTERNATE ON
  SET CONSOLE OFF
  convertVPER(prefix, pfad, schema)
  SET CONSOLE ON
  
  SET ALTERNATE OFF
  ? "Konvertiere VAMT ... "
  SET ALTERNATE ON
  SET CONSOLE OFF
  convertVAMT(prefix, pfad, schema)
  SET CONSOLE ON
  
  SET ALTERNATE OFF
  ? "Konvertiere VFAM ... "
  SET ALTERNATE ON
  SET CONSOLE OFF
  convertVFAM(prefix, pfad, schema)
  SET CONSOLE ON
  
  SET ALTERNATE OFF
  ? "Konvertiere VORT ... "
  SET ALTERNATE ON
  SET CONSOLE OFF
  convertVORT(prefix, pfad, schema)
  SET CONSOLE ON
  
  SET ALTERNATE OFF
  ? "Konvertiere VVWG ... "
  SET ALTERNATE ON
  SET CONSOLE OFF
  convertVVWG(prefix, pfad, schema)
  SET CONSOLE ON

  ?
  
  SET ALTERNATE OFF
    
RETURN


PROCEDURE convertVGEB(prefix, pfad, schema)

  vgebold = pfad + prefix + "_vgeb"
  
  sql = "CREATE TABLE " + schema + ".vgeb ("
  sql = sql +  "x_recno int4 PRIMARY KEY," 
  sql = sql +  "x_ind char(1),"
  sql = sql +  "x_stand date,"
  sql = sql +  "x_vamt int4,"
  sql = sql +  "x_urk varchar(11),"
  sql = sql +  "x_stat char(1),"
  sql = sql +  "x_dat_kz char(1),"
  sql = sql +  "x_udatfr varchar(12),"
  sql = sql +  "x_udatum varchar(8),"
  sql = sql +  "g_evk int4,"
  sql = sql +  "g_geb_dfr varchar(12),"
  sql = sql +  "g_geb_d varchar(8),"
  sql = sql +  "g_geb_std varchar(5),"
  sql = sql +  "g_kind int4,"
  sql = sql +  "g_v_kd int4,"
  sql = sql +  "g_m_kd int4,"
  sql = sql +  "g_h_dat varchar(8),"
  sql = sql +  "g_h_ort int4,"
  sql = sql +  "x_pers1 int4,"
  sql = sql +  "x_pers2 int4,"
  sql = sql +  "x_pers3 int4,"
  sql = sql +  "x_pers4 int4,"
  sql = sql +  "x_pers5 int4,"
  sql = sql +  "x_pers6 int4,"
  sql = sql +  "x_pers7 int4,"
  sql = sql +  "x_pers8 int4,"
  sql = sql +  "x_pers9 int4,"
  sql = sql +  "x_persa int4,"
  sql = sql +  "x_persb int4,"
  sql = sql +  "x_persc int4,"
  sql = sql +  "x_bem text"
  sql = sql +  ");"

  ? sql

  USE &vgebold NEW ALIAS vgeb_old
  GOTO TOP
  DO WHILE .not. vgeb_old->(eof())

     sql = "INSERT INTO " + schema + ".vgeb (x_recno, x_ind, x_stand, x_vamt, x_urk, x_stat, x_dat_kz, x_udatfr, x_udatum, g_evk, g_geb_dfr, g_geb_d, g_geb_std, g_kind, g_v_kd, g_m_kd, g_h_dat, g_h_ort, x_pers1, x_pers2, x_pers3, x_pers4, x_pers5, x_pers6, x_pers7, x_pers8, x_pers9, x_persa, x_persb, x_persc, x_bem) VALUES ("
     sql = sql + str(vgeb_old->(recno())) + ","
     sql = sql + "'" + trim(vgeb_old->X_IND) + "', "
     sql = sql + "null,"
     // sql = sql + trim(vgeb_old->X_STAND) + ", "
     sql = sql + str(convertRecnoToLong (vgeb_old->X_VAMT))+ ", "
     sql = sql + "'" + trim(vgeb_old->X_URK) + "', "
     sql = sql + "'" + trim(vgeb_old->X_STAT) + "', "
     sql = sql + "'" + trim(vgeb_old->X_DAT_KZ) + "', "
     sql = sql + "'" + trim(vgeb_old->X_UDATFR) + "', "
     sql = sql + "'" + trim(vgeb_old->X_UDATUM) + "', "
    
     sql = sql + str(convertRecnoToLong(vgeb_old->G_EVK)) + ", "
     sql = sql + "'" + trim(vgeb_old->G_GEB_DFR) + "', "
     sql = sql + "'" + trim(vgeb_old->G_GEB_D) + "', "
     sql = sql + "'" + trim(vgeb_old->G_GEB_STD) + "', "
     sql = sql + str(convertRecnoToLong (vgeb_old->G_KIND)) + ", "
     sql = sql + str(convertRecnoToLong (vgeb_old->G_V_KD)) + ", "
     sql = sql + str(convertRecnoToLong (vgeb_old->G_M_KD)) + ", "
     sql = sql + "'" + trim(vgeb_old->G_H_DAT) + "', "
     sql = sql + str(convertRecnoToLong (vgeb_old->G_H_ORT)) + ", "
    
     sql = sql + str(convertRecnoToLong (vgeb_old->X_PERS1)) + ", "
     sql = sql + str(convertRecnoToLong (vgeb_old->X_PERS2)) + ", "
     sql = sql + str(convertRecnoToLong (vgeb_old->X_PERS3)) + ", "
     sql = sql + str(convertRecnoToLong (vgeb_old->X_PERS4)) + ", "
     sql = sql + str(convertRecnoToLong (vgeb_old->X_PERS5)) + ", "
     sql = sql + str(convertRecnoToLong (vgeb_old->X_PERS6)) + ", "
     sql = sql + str(convertRecnoToLong (vgeb_old->X_PERS7)) + ", "
     sql = sql + str(convertRecnoToLong (vgeb_old->X_PERS8)) + ", "
     sql = sql + str(convertRecnoToLong (vgeb_old->X_PERS9)) + ", "
     sql = sql + str(convertRecnoToLong (vgeb_old->X_PERSA)) + ", "
     sql = sql + str(convertRecnoToLong (vgeb_old->X_PERSB)) + ", "
     sql = sql + str(convertRecnoToLong (vgeb_old->X_PERSC)) + ", "
     sql = sql + "'" + trim(checkHochkomma(vgeb_old->X_BEM)) + "');"

    ? sql
    SKIP

  ENDDO

RETURN


PROCEDURE convertVHEI(prefix, pfad, schema)

  vheiold = pfad + prefix + "_vhei"

  sql = "CREATE TABLE " + schema + ".vhei ("
  sql = sql + "x_recno integer PRIMARY KEY, "
  sql = sql + "x_ind character(1), "
  sql = sql + "x_stand date, "
  sql = sql + "x_vamt integer, "
  sql = sql + "x_urk character varying(11),"
  sql = sql + "x_stat character(1), "
  sql = sql + "x_dat_kz character varying(11), "
  sql = sql + "x_udatfr character varying(12), "
  sql = sql + "x_udatum character varying(8),"
  sql = sql + "h_ebg integer, "
  sql = sql + "h_evbg integer, "
  sql = sql + "h_evbt integer, "
  sql = sql + "h_hei_dfr character varying(12), "
  sql = sql + "h_hei_d character varying(8),"
  sql = sql + "h_hei_ort integer, "
  sql = sql + "h_bg integer, "
  sql = sql + "h_bg_v integer, "
  sql = sql + "h_bg_m integer, "
  sql = sql + "h_bt integer, "
  sql = sql + "h_bt_v integer, "
  sql = sql + "h_bt_m integer,"
  sql = sql + "x_pers1 integer, "
  sql = sql + "x_pers2 integer, "
  sql = sql + "x_pers3 integer, "
  sql = sql + "x_pers4 integer, "
  sql = sql + "x_pers5 integer, "
  sql = sql + "x_pers6 integer,"
  sql = sql + "x_pers7 integer, "
  sql = sql + "x_pers8 integer, "
  sql = sql + "x_pers9 integer, "
  sql = sql + "x_persa integer, "
  sql = sql + "x_persb integer, "
  sql = sql + "x_persc integer, "
  sql = sql + "x_bem text);"

  ? sql
  
  USE &vheiold NEW ALIAS vhei_old
  GOTO TOP

  DO WHILE .not. vhei_old->(eof())

    sql = "INSERT INTO " + schema + ".vhei (x_recno, x_ind, x_stand, x_vamt, x_urk, x_stat, x_dat_kz, x_udatfr, x_udatum, h_ebg, h_evbg, h_evbt, h_hei_dfr, h_hei_d, h_hei_ort, h_bg, h_bg_v, h_bg_m, h_bt, h_bt_v, h_bt_m, x_pers1, x_pers2, x_pers3, x_pers4, x_pers5, x_pers6, x_pers7, x_pers8, x_pers9, x_persa, x_persb, x_persc, x_bem) VALUES ("
    sql = sql + str(vhei_old->(recno())) + ","
    sql = sql + "'" + trim(vhei_old->X_IND) + "',"
    sql = sql + "null,"
    // sql = sql + vhei_old->X_STAND
    sql = sql + str(convertRecnoToLong (vhei_old->X_VAMT)) + ","
    sql = sql + "'" + trim(vhei_old->X_URK) + "',"
    sql = sql + "'" + trim(vhei_old->X_STAT) + "',"
    sql = sql + "'" + trim(vhei_old->X_DAT_KZ) + "',"
    sql = sql + "'" + trim(vhei_old->X_UDATFR) + "',"
    sql = sql + "'" + trim(vhei_old->X_UDATUM) + "',"

    sql = sql + str(convertRecnoToLong (vhei_old->H_EBG)) + ","
    sql = sql + str(convertRecnoToLong (vhei_old->H_EVBG)) + ","
    sql = sql + str(convertRecnoToLong (vhei_old->H_EVBT)) + ","

    sql = sql + "'" + trim(vhei_old->H_HEI_DFR) + "',"
    sql = sql + "'" + trim(vhei_old->H_HEI_D) + "',"
    sql = sql + str(convert2RecnoToLong (vhei_old->H_HEI_ORT)) + "," // Spezialbehandlung wegen char(3) Feld
    sql = sql + str(convertRecnoToLong (vhei_old->H_BG)) + ","    
    sql = sql + str(convertRecnoToLong (vhei_old->H_BG_V)) + ","    
    sql = sql + str(convertRecnoToLong (vhei_old->H_BG_M)) + ","    
    sql = sql + str(convertRecnoToLong (vhei_old->H_BT)) + ","    
    sql = sql + str(convertRecnoToLong (vhei_old->H_BT_V)) + ","    
    sql = sql + str(convertRecnoToLong (vhei_old->H_BT_M)) + ","    

    sql = sql + str(convertRecnoToLong (vhei_old->X_PERS1)) + ","
    sql = sql + str(convertRecnoToLong (vhei_old->X_PERS2)) + ","
    sql = sql + str(convertRecnoToLong (vhei_old->X_PERS3)) + ","
    sql = sql + str(convertRecnoToLong (vhei_old->X_PERS4)) + ","
    sql = sql + str(convertRecnoToLong (vhei_old->X_PERS5)) + ","
    sql = sql + str(convertRecnoToLong (vhei_old->X_PERS6)) + ","
    sql = sql + str(convertRecnoToLong (vhei_old->X_PERS7)) + ","
    sql = sql + str(convertRecnoToLong (vhei_old->X_PERS8)) + ","
    sql = sql + str(convertRecnoToLong (vhei_old->X_PERS9)) + ","
    sql = sql + str(convertRecnoToLong (vhei_old->X_PERSA)) + ","
    sql = sql + str(convertRecnoToLong (vhei_old->X_PERSB)) + ","
    sql = sql + str(convertRecnoToLong (vhei_old->X_PERSC)) + ","
    sql = sql + "'" + checkHochkomma(vhei_old->X_BEM) + "');"

    ? sql
    
    SKIP

  ENDDO

  USE
  
RETURN


PROCEDURE convertVSTE (prefix, pfad, schema)
  
  local begOrtFound
  
  begOrtFound = 0
  
  vsteold = pfad + prefix + "_vste"

  sql = "CREATE TABLE " + schema + ".vste ("
  sql = sql + "x_recno integer PRIMARY KEY,"
  sql = sql + "x_ind character(1),"
  sql = sql + "x_stand date,"
  sql = sql + "x_vamt integer,"
  sql = sql + "x_urk character varying(11),"
  sql = sql + "x_stat character(1),"
  sql = sql + "x_dat_kz character(1),"
  sql = sql + "x_udatfr character varying(12),"
  sql = sql + "x_udatum character varying(8),"
  sql = sql + "s_evst integer,"
  sql = sql + "s_evvst integer,"
  sql = sql + "s_ste_dfr character varying(12),"
  sql = sql + "s_ste_d character varying(8),"
  sql = sql + "s_ste_std character varying(5),"
  sql = sql + "s_beg_d character varying(8),"
  sql = sql + "s_beg_ort integer,"
  sql = sql + "s_vst integer,"
  sql = sql + "s_vst_ep integer,"
  sql = sql + "s_vst_v integer,"
  sql = sql + "s_vst_m integer,"
  sql = sql + "s_t_urs character varying(28),"
  sql = sql + "x_pers1 integer,"
  sql = sql + "x_pers2 integer,"
  sql = sql + "x_pers3 integer,"
  sql = sql + "x_pers4 integer,"
  sql = sql + "x_pers5 integer,"
  sql = sql + "x_pers6 integer,"
  sql = sql + "x_pers7 integer,"
  sql = sql + "x_pers8 integer,"
  sql = sql + "x_pers9 integer,"
  sql = sql + "x_persa integer,"
  sql = sql + "x_persb integer,"
  sql = sql + "x_persc integer,"
  sql = sql + "x_bem text);"

  ? sql
  
  USE &vsteold NEW ALIAS vste_old
  GOTO TOP

  
  FOR nField := 1 TO FCOUNT()
    IF FIELDNAME(nField) = "S_BEG_ORT"
      begOrtFound = 1
    ENDIF
  NEXT

  DO WHILE .not. vste_old->(eof())

    IF begOrtFound = 1
      sql = "INSERT INTO " + schema + ".vste (x_recno, x_ind, x_stand, x_vamt, x_urk, x_stat, x_dat_kz, x_udatfr, x_udatum, s_evst, s_evvst, s_ste_dfr, s_ste_d, s_ste_std, s_beg_d, s_beg_ort, s_vst, s_vst_ep, s_vst_v, s_vst_m, s_t_urs, x_pers1, x_pers2, x_pers3, x_pers4, x_pers5, x_pers6, x_pers7, x_pers8, x_pers9, x_persa, x_persb, x_persc, x_bem) VALUES ("
    ELSE
      sql = "INSERT INTO " + schema + ".vste (x_recno, x_ind, x_stand, x_vamt, x_urk, x_stat, x_dat_kz, x_udatfr, x_udatum, s_evst, s_evvst, s_ste_dfr, s_ste_d, s_ste_std, s_beg_d, s_vst, s_vst_ep, s_vst_v, s_vst_m, s_t_urs, x_pers1, x_pers2, x_pers3, x_pers4, x_pers5, x_pers6, x_pers7, x_pers8, x_pers9, x_persa, x_persb, x_persc, x_bem) VALUES ("
    ENDIF
    
    sql = sql + str(vste_old->(recno())) + ","
    sql = sql + "'" + trim(vste_old->X_IND) + "',"
    sql = sql + "null,"
    // sql = sql + "'" + vste_old->X_STAND + "',"
    sql = sql + str(convertRecnoToLong (vste_old->X_VAMT)) + ","
    sql = sql + "'" + trim(vste_old->X_URK) + "',"
    sql = sql + "'" + trim(vste_old->X_STAT) + "',"
    sql = sql + "'" + trim(vste_old->X_DAT_KZ) + "',"
    sql = sql + "'" + trim(vste_old->X_UDATFR) + "',"
    sql = sql + "'" + trim(vste_old->X_UDATUM) + "',"

    sql = sql + str(convertRecnoToLong (vste_old->S_EVST)) + ","
    sql = sql + str(convertRecnoToLong (vste_old->S_EVVST)) + ","
    sql = sql + "'" + trim(vste_old->S_STE_DFR) + "',"
    sql = sql + "'" + trim(vste_old->S_STE_D) + "',"
    sql = sql + "'" + trim(vste_old->S_STE_STD) + "',"
    sql = sql + "'" + trim(vste_old->S_BEG_D) + "',"
    
    IF begOrtFound = 1
      sql = sql + str(convertRecnoToLong (vste_old->S_BEG_ORT)) + ","
    ELSE
      sql = sql + "null" + ","
    ENDIF
    
    sql = sql + str(convertRecnoToLong (vste_old->S_VST)) + ","
    sql = sql + str(convertRecnoToLong (vste_old->S_VST_EP)) + ","
    sql = sql + str(convertRecnoToLong (vste_old->S_VST_V)) + ","
    sql = sql + str(convertRecnoToLong (vste_old->S_VST_M)) + ","
    sql = sql + "'" + trim(vste_old->S_T_URS) + "',"

    sql = sql + str(convertRecnoToLong (vste_old->X_PERS1)) + ","
    sql = sql + str(convertRecnoToLong (vste_old->X_PERS2)) + ","
    sql = sql + str(convertRecnoToLong (vste_old->X_PERS3)) + ","
    sql = sql + str(convertRecnoToLong (vste_old->X_PERS4)) + ","
    sql = sql + str(convertRecnoToLong (vste_old->X_PERS5)) + ","
    sql = sql + str(convertRecnoToLong (vste_old->X_PERS6)) + ","
    sql = sql + str(convertRecnoToLong (vste_old->X_PERS7)) + ","
    sql = sql + str(convertRecnoToLong (vste_old->X_PERS8)) + ","
    sql = sql + str(convertRecnoToLong (vste_old->X_PERS9)) + ","
    sql = sql + str(convertRecnoToLong (vste_old->X_PERSA)) + ","
    sql = sql + str(convertRecnoToLong (vste_old->X_PERSB)) + ","
    sql = sql + str(convertRecnoToLong (vste_old->X_PERSC)) + ","
    sql = sql + "'" + checkHochkomma(vste_old->X_BEM) + "');"

    ? sql
    
    SKIP

  ENDDO

  USE

RETURN


PROCEDURE convertVPER(prefix, pfad, schema)

  vperold = pfad + prefix + "_vper"

    sql = "CREATE TABLE " + schema + ".vper ("
    sql = sql + "x_recno integer PRIMARY KEY,"
    sql = sql + "x_ind character(1),"
    sql = sql + "x_stand date,"
    sql = sql + "x_zurk integer,"
    sql = sql + "x_stat character(1),"
    sql = sql + "p_reg character(1),"
    sql = sql + "p_event_d character varying(8),"
    sql = sql + "p_eltern integer,"
    sql = sql + "p_ehe integer,"
    sql = sql + "p_bruder integer,"
    sql = sql + "p_ket1 integer,"
    sql = sql + "p_ketn integer,"
    sql = sql + "p_aper integer,"
    sql = sql + "p_stpers integer,"
    sql = sql + "p_verwgr integer,"
    sql = sql + "p_sex character(1),"
    sql = sql + "p_name character varying(30),"
    sql = sql + "p_vname character varying(33),"
    sql = sql + "p_beruf character varying(30),"
    sql = sql + "p_ledig character varying(9),"
    sql = sql + "p_konf character(2),"
    sql = sql + "p_geb_dat character varying(8),"
    sql = sql + "p_geb_d_s character(1),"
    sql = sql + "p_geb_ort integer,"
    sql = sql + "p_tau_dat character varying(8),"
    sql = sql + "p_tau_ort integer,"
    sql = sql + "p_ste_dat character varying(8),"
    sql = sql + "p_ste_ort integer,"
    sql = sql + "p_beg_dat character varying(8),"
    sql = sql + "p_beg_ort integer,"
    sql = sql + "p_wohnort integer,"
    sql = sql + "p_her_ort integer,"
    sql = sql + "p_alter_j character(3),"
    sql = sql + "p_alter_m character(2),"
    sql = sql + "p_alter_w character(2),"
    sql = sql + "p_alter_t character(2),"
    sql = sql + "x_bem text);"

  ? sql
  USE &vperold NEW ALIAS vper_old

  GOTO TOP

  DO WHILE .not. vper_old->(eof())

    sql = "INSERT INTO " + schema + ".vper (x_recno, x_ind, x_stand, x_zurk, x_stat, p_reg, p_event_d, p_eltern, p_ehe, p_bruder, p_ket1, p_ketn, p_aper, p_stpers, p_verwgr, p_sex, p_name, p_vname, p_beruf, p_ledig, p_konf, p_geb_dat, p_geb_d_s, p_geb_ort, p_tau_dat, p_tau_ort, p_ste_dat, p_ste_ort, p_beg_dat, p_beg_ort, p_wohnort, p_her_ort, p_alter_j, p_alter_m, p_alter_w, p_alter_t, x_bem) VALUES ("

    sql = sql + str(vper_old->(recno())) + ","
    sql = sql + "'" + trim(vper_old->X_IND) + "',"
    // sql = sql + "'" + vper_old->X_STAND + "',"
    sql = sql + "null,"
    sql = sql + str(convertRecnoToLong (vper_old->X_ZURK)) + ","
    sql = sql + "'" + trim(vper_old->X_STAT) + "',"
    sql = sql + "'" + trim(vper_old->P_REG) + "',"
    sql = sql + "'" + trim(vper_old->P_EVENT_D) + "',"
    sql = sql + str(convertRecnoToLong (vper_old->P_ELTERN)) + ","
    sql = sql + str(convertRecnoToLong (vper_old->P_EHE)) + ","
    sql = sql + str(convertRecnoToLong (vper_old->P_BRUDER)) + ","
    sql = sql + str(convertRecnoToLong (vper_old->P_KET1)) + ","
    sql = sql + str(convertRecnoToLong (vper_old->P_KETN)) + ","
    sql = sql + str(convertRecnoToLong (vper_old->P_APER)) + ","
    sql = sql + str(convertRecnoToLong (vper_old->P_STPERS)) + ","
    sql = sql + str(convertRecnoToLong (vper_old->P_VERWGR)) + ","
    sql = sql + "'" + trim(vper_old->P_SEX) + "',"
    sql = sql + "'" + trim(checkHochkomma(vper_old->P_NAME)) + "',"
    sql = sql + "'" + trim(checkHochkomma(vper_old->P_VNAME)) + "',"
    sql = sql + "'" + trim(checkHochkomma(vper_old->P_BERUF)) + "',"
    sql = sql + "'" + trim(vper_old->P_LEDIG) + "',"
    sql = sql + "'" + trim(vper_old->P_KONF) + "',"
    sql = sql + "'" + trim(vper_old->P_GEB_DAT) + "',"
    sql = sql + "'" + trim(vper_old->P_GEB_D_S) + "',"
    sql = sql + str(convertRecnoToLong (vper_old->P_GEB_ORT)) + ","
    sql = sql + "'" + trim(vper_old->P_TAU_DAT) + "',"
    sql = sql + str(convertRecnoToLong (vper_old->P_TAU_ORT)) + ","
    sql = sql + "'" + trim(vper_old->P_STE_DAT) + "',"
    sql = sql + str(convertRecnoToLong (vper_old->P_STE_ORT)) + ","
    sql = sql + "'" + trim(vper_old->P_BEG_DAT) + "',"
    sql = sql + str(convertRecnoToLong (vper_old->P_BEG_ORT)) + ","
    sql = sql + str(convertRecnoToLong (vper_old->P_WOHNORT)) + ","
    sql = sql + str(convertRecnoToLong (vper_old->P_HER_ORT)) + ","
    sql = sql + "'" + trim(vper_old->P_ALTER_J) + "',"
    sql = sql + "'" + trim(vper_old->P_ALTER_M) + "',"
    sql = sql + "'" + trim(vper_old->P_ALTER_W) + "',"
    sql = sql + "'" + trim(vper_old->P_ALTER_T) + "',"
    sql = sql + "'" + trim(checkHochkomma(vper_old->X_BEM)) + "');"

    ? sql
    SKIP

  ENDDO

  USE

  ? "CREATE INDEX vper_name ON " + schema + ".vper(p_name, p_vname);"

RETURN


PROCEDURE convertVAMT(prefix, pfad, schema)

  vamtold = pfad + prefix + "_vamt"

  sql = "CREATE TABLE " + schema + ".vamt ("
  sql = sql + "x_recno integer PRIMARY KEY,"
  sql = sql + "x_ind character(1),"
  sql = sql + "a_kuerz character(5),"
  sql = sql + "a_konf character(2),"
  sql = sql + "a_ort character(20),"
  sql = sql + "a_amt character(20),"
  sql = sql + "a_ao character(2),"
  sql = sql + "a_aa character(2));"

  ? sql 
  
  USE &vamtold NEW ALIAS vamt_old

  GOTO TOP

  DO WHILE .not. vamt_old->(eof())

    sql = "INSERT INTO " + schema + ".vamt (x_recno, x_ind, a_kuerz, a_konf, a_ort, a_amt, a_ao, a_aa) VALUES ("
    sql = sql + str(vamt_old->(RECNO())) + ","
    sql = sql + "'" + trim(vamt_old->X_IND) + "',"
    sql = sql + "'" + trim(vamt_old->A_KUERZ) + "',"
    sql = sql + "'" + trim(vamt_old->A_KONF) + "',"
    sql = sql + "'" + trim(checkHochkomma(vamt_old->A_ORT)) + "',"  
    sql = sql + "'" + trim(checkHochkomma(vamt_old->A_AMT)) + "',"
    sql = sql + "'" + trim(vamt_old->A_AO) + "',"
    sql = sql + "'" + trim(vamt_old->A_AA) + "');"

    ? sql
    
    SKIP

  ENDDO

  USE

RETURN


PROCEDURE convertVFAM(prefix, pfad, schema)

  vfamold = pfad + prefix + "_vfam"

  sql = "CREATE TABLE " + schema + ".vfam ("
  sql = sql + "x_recno integer PRIMARY KEY,"
  sql = sql + "x_ind character(1),"
  sql = sql + "x_stat character(1),"
  sql = sql + "f_vater integer,"
  sql = sql + "f_mutter integer,"
  sql = sql + "f_mfg1 integer,"
  sql = sql + "f_mfgn integer,"
  sql = sql + "f_vatweh1 integer,"
  sql = sql + "f_vatwehe integer,"
  sql = sql + "f_mutweh1 integer,"
  sql = sql + "f_mutwehe integer,"
  sql = sql + "f_ket1 integer,"
  sql = sql + "f_ketn integer,"
  sql = sql + "f_kind integer,"
  sql = sql + "f_vmehn integer,"
  sql = sql + "f_vmeh1 integer,"
  sql = sql + "f_mmeh1 integer,"
  sql = sql + "f_mmehn integer,"
  sql = sql + "f_afam integer);"

  ? sql
  
  USE &vfamold NEW ALIAS vfam_old

  GOTO TOP

  DO WHILE .not. vfam_old->(eof())

    sql = "INSERT INTO " + schema + ".vfam (x_recno, x_ind, x_stat, f_vater, f_mutter, f_mfg1, f_mfgn, f_vatweh1, f_vatwehe, f_mutweh1, f_mutwehe, f_ket1, f_ketn, f_kind, f_vmehn, f_vmeh1, f_mmeh1, f_mmehn, f_afam) VALUES ("
    sql = sql + str(vfam_old->(RECNO())) + ","
    sql = sql + "'" + trim(vfam_old->X_IND) + "',"
    sql = sql + "'" + trim(vfam_old->X_STAT) + "',"
    sql = sql + str(convertRecnoToLong (vfam_old->F_VATER)) + ","
    sql = sql + str(convertRecnoToLong (vfam_old->F_MUTTER)) + ","
    sql = sql + str(convertRecnoToLong (vfam_old->F_KIND)) + ","
    sql = sql + str(convertRecnoToLong (vfam_old->F_MFG1)) + ","
    sql = sql + str(convertRecnoToLong (vfam_old->F_MFGN)) + ","
    sql = sql + str(convertRecnoToLong (vfam_old->F_VATWEH1)) + ","
    sql = sql + str(convertRecnoToLong (vfam_old->F_VATWEHE)) + ","
    sql = sql + str(convertRecnoToLong (vfam_old->F_MUTWEH1)) + ","
    sql = sql + str(convertRecnoToLong (vfam_old->F_MUTWEHE)) + ","
    sql = sql + str(convertRecnoToLong (vfam_old->F_KET1)) + ","
    sql = sql + str(convertRecnoToLong (vfam_old->F_KETN)) + ","
    sql = sql + str(convertRecnoToLong (vfam_old->F_VMEH1)) + ","
    sql = sql + str(convertRecnoToLong (vfam_old->F_VMEHN)) + ","
    sql = sql + str(convertRecnoToLong (vfam_old->F_MMEH1)) + ","
    sql = sql + str(convertRecnoToLong (vfam_old->F_MMEHN)) + ","
    sql = sql + str(convertRecnoToLong (vfam_old->F_AFAM)) + ");"

    ? sql
    SKIP

  ENDDO

  USE

RETURN


PROCEDURE convertVORT(prefix, pfad, schema)

  vortold = pfad + prefix + "_vort"

  sql = "CREATE TABLE " + schema + ".vort ("
  sql = sql + "x_recno integer PRIMARY KEY,"
  sql = sql + "x_ind character(1),"
  sql = sql + "x_stat character(1),"
  sql = sql + "o_ort character(20),"
  sql = sql + "o_erg character(35),"
  sql = sql + "o_aort integer);"

  ? sql
  USE &vortold NEW ALIAS vort_old

  GOTO TOP
        
  DO WHILE .not. vort_old->(eof())

    sql = "INSERT INTO " + schema + ".vort (x_recno, x_ind, x_stat, o_ort, o_erg, o_aort) VALUES ("
    sql = sql + str(vort_old->(RECNO())) + ","
    sql = sql + "'" + trim(vort_old->X_IND) + "',"
    sql = sql + "'" + trim(vort_old->X_STAT) + "',"
    sql = sql + "'" + trim(checkHochkomma(vort_old->O_ORT)) + "',"                         
    sql = sql + "'" + trim(checkHochkomma(vort_old->O_ERG)) + "',"                                  
    sql = sql + str(convertRecnoToLong(vort_old->O_AORT)) + ");"

    ? sql
    SKIP

  ENDDO

  USE

  ? "CREATE INDEX vort_name ON " + schema + ".vort USING btree (o_ort);"

RETURN


PROCEDURE convertVVWG(prefix, pfad, schema)

  vvwgold = pfad + prefix + "_vvwg"

  sql = "CREATE TABLE " + schema + ".vvwg ("
  sql = sql + "x_recno integer PRIMARY KEY,"
  sql = sql + "x_ind character(1),"
  sql = sql + "x_stat character(1),"
  sql = sql + "v_vgr character(8),"
  sql = sql + "v_text character(35));"

  ? sql
  
  USE &vvwgold NEW ALIAS vvwg_old

  GOTO TOP

  DO WHILE .not. vvwg_old->(eof())

    sql = "INSERT INTO " + schema + ".vvwg (x_recno, x_ind, x_stat, v_vgr, v_text) VALUES ("
    sql = sql + str(vvwg_old->(RECNO())) + ","
    sql = sql + "'" + trim(vvwg_old->X_IND) + "',"
    sql = sql + "'" + trim(vvwg_old->X_STAT) + "',"
    sql = sql + "'" + trim(vvwg_old->V_VGR) + "',"                                 
    sql = sql + "'" + trim(vvwg_old->V_TEXT) + "');"                                 

    ? sql
    SKIP

  ENDDO

  USE

RETURN


PROCEDURE convertRecnoToLong(char)

  IF (len(char) = 3)
    ret3 = asc (right (char,1  )) - 32
    ret2 = asc (substr(char,2,1)) - 32
    ret1 = asc (left  (char,1  )) - 32

    ret = (ret1 * 222 + ret2) * 222 + ret3
  ELSE
    IF (len(char) = 2)
      ret2 = asc (right (char,1  )) - 32
      ret1 = asc (left  (char,1  )) - 32

      ret = ret1 * 222 + ret2
    ELSE
      ? "ERROR"
    ENDIF
  ENDIF

RETURN ret

// Spezialbehandlung wegen char(3) Feld bei h_hei_ort
PROCEDURE convert2RecnoToLong(char)

    ret2 = asc (substr(char,2,1)) - 32
    ret1 = asc (left  (char,1  )) - 32

    ret = ret1 * 222 + ret2

RETURN ret

PROCEDURE checkHochkomma(string)

  string = STRTRAN(string, "'", "''")

RETURN  string

PROCEDURE vkhelp()

  ? "VKMEXPORT - VKM EXPORT nach SQL"
  ? "(c) 2003 Dirk E. Wagner"
  ? 
  ? "Syntax: VKMEXPORT <Dateikürzel> <Pfad> <DBSchema>"
  ?
  ? "Exportiert VK Daten in SQL-Script"
  ?

return
