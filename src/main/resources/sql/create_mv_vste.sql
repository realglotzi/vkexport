DROP MATERIALIZED VIEW IF EXISTS mv_vste_resolved;

CREATE MATERIALIZED VIEW mv_vste_resolved AS
SELECT
  -- Basis
  v.x_recno                               AS vste_id,
  v.x_urk                                 AS urk,
  v.x_dat_kz                              AS dat_kz,
  v.x_udatfr                              AS udatfr,
  v.x_udatum                              AS udatum,

  -- Sterbedatum/-zeit
  v.s_ste_dfr                             AS sterbe_dfr,
  v.s_ste_d                               AS sterbe_datum,
  v.s_ste_std                             AS sterbe_stunde,

  -- Bestattung: Datum + Ort (nur Name)
  v.s_beg_d                               AS bestattungs_datum,
  ort.o_ort                               AS bestattungs_ort_name,

  -- Verstorbene*r
  z.p_name                                AS verstorbene_name,
  z.p_vname                               AS verstorbene_vorname,
  wz.v_text                               AS verstorbene_verwgr_text,

  -- Ehepartner (falls vorhanden)
  ep.p_name                               AS ep_name,
  ep.p_vname                              AS ep_vorname,
  wep.v_text                              AS ep_verwgr_text,

  -- Vater/Mutter (nur Namen, wie bei vhei ohne Sex)
  zv.p_name                               AS vater_name,
  zv.p_vname                              AS vater_vorname,
  zm.p_name                               AS mutter_name,
  zm.p_vname                              AS mutter_vorname,

  -- Teilnehmer-Slots 1..9,a..c: Name/Vorname + Verwgr-Text
  p1.p_name                               AS pers1_name,
  p1.p_vname                              AS pers1_vorname,
  w1.v_text                               AS pers1_verwgr_text,

  p2.p_name                               AS pers2_name,
  p2.p_vname                              AS pers2_vorname,
  w2.v_text                               AS pers2_verwgr_text,

  p3.p_name                               AS pers3_name,
  p3.p_vname                              AS pers3_vorname,
  w3.v_text                               AS pers3_verwgr_text,

  p4.p_name                               AS pers4_name,
  p4.p_vname                              AS pers4_vorname,
  w4.v_text                               AS pers4_verwgr_text,

  p5.p_name                               AS pers5_name,
  p5.p_vname                              AS pers5_vorname,
  w5.v_text                               AS pers5_verwgr_text,

  p6.p_name                               AS pers6_name,
  p6.p_vname                              AS pers6_vorname,
  w6.v_text                               AS pers6_verwgr_text,

  p7.p_name                               AS pers7_name,
  p7.p_vname                              AS pers7_vorname,
  w7.v_text                               AS pers7_verwgr_text,

  p8.p_name                               AS pers8_name,
  p8.p_vname                              AS pers8_vorname,
  w8.v_text                               AS pers8_verwgr_text,

  p9.p_name                               AS pers9_name,
  p9.p_vname                              AS pers9_vorname,
  w9.v_text                               AS pers9_verwgr_text,

  pa.p_name                               AS persa_name,
  pa.p_vname                              AS persa_vorname,
  wa.v_text                               AS persa_verwgr_text,

  pb.p_name                               AS persb_name,
  pb.p_vname                              AS persb_vorname,
  wb.v_text                               AS persb_verwgr_text,

  pc.p_name                               AS persc_name,
  pc.p_vname                              AS persc_vorname,
  wc.v_text                               AS persc_verwgr_text,

  -- Bemerkung
  v.x_bem                                 AS bem

FROM vste v
-- Verstorbene*r, Partner, Eltern
LEFT JOIN vper z    ON z.x_recno   = v.s_vst
LEFT JOIN vper ep   ON ep.x_recno  = v.s_vst_ep
LEFT JOIN vper zv   ON zv.x_recno  = v.s_vst_v
LEFT JOIN vper zm   ON zm.x_recno  = v.s_vst_m

-- Verwgr-Text der Hauptpersonen
LEFT JOIN vvwg wz   ON wz.x_recno  = z.p_verwgr
LEFT JOIN vvwg wep  ON wep.x_recno = ep.p_verwgr

-- Bestattungsort (nur Name)
LEFT JOIN vort ort  ON ort.x_recno = v.s_beg_ort

-- Teilnehmer + Verwgr
LEFT JOIN vper p1   ON p1.x_recno  = v.x_pers1
LEFT JOIN vvwg w1   ON w1.x_recno  = p1.p_verwgr
LEFT JOIN vper p2   ON p2.x_recno  = v.x_pers2
LEFT JOIN vvwg w2   ON w2.x_recno  = p2.p_verwgr
LEFT JOIN vper p3   ON p3.x_recno  = v.x_pers3
LEFT JOIN vvwg w3   ON w3.x_recno  = p3.p_verwgr
LEFT JOIN vper p4   ON p4.x_recno  = v.x_pers4
LEFT JOIN vvwg w4   ON w4.x_recno  = p4.p_verwgr
LEFT JOIN vper p5   ON p5.x_recno  = v.x_pers5
LEFT JOIN vvwg w5   ON w5.x_recno  = p5.p_verwgr
LEFT JOIN vper p6   ON p6.x_recno  = v.x_pers6
LEFT JOIN vvwg w6   ON w6.x_recno  = p6.p_verwgr
LEFT JOIN vper p7   ON p7.x_recno  = v.x_pers7
LEFT JOIN vvwg w7   ON w7.x_recno  = p7.p_verwgr
LEFT JOIN vper p8   ON p8.x_recno  = v.x_pers8
LEFT JOIN vvwg w8   ON w8.x_recno  = p8.p_verwgr
LEFT JOIN vper p9   ON p9.x_recno  = v.x_pers9
LEFT JOIN vvwg w9   ON w9.x_recno  = p9.p_verwgr
LEFT JOIN vper pa   ON pa.x_recno  = v.x_persa
LEFT JOIN vvwg wa   ON wa.x_recno  = pa.p_verwgr
LEFT JOIN vper pb   ON pb.x_recno  = v.x_persb
LEFT JOIN vvwg wb   ON wb.x_recno  = pb.p_verwgr
LEFT JOIN vper pc   ON pc.x_recno  = v.x_persc
LEFT JOIN vvwg wc   ON wc.x_recno  = pc.p_verwgr
WITH NO DATA;

-- Eindeutiger Schlüssel (für CONCURRENTLY Refresh)
CREATE UNIQUE INDEX IF NOT EXISTS mv_vste_resolved_uq
  ON mv_vste_resolved (vste_id);

-- (optional) typischer Filter-Index
CREATE INDEX IF NOT EXISTS mv_vste_resolved_sterbe_idx
  ON mv_vste_resolved (sterbe_datum);

-- Erstbefüllung
REFRESH MATERIALIZED VIEW mv_vste_resolved;
