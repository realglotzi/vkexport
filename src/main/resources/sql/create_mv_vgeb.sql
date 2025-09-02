DROP MATERIALIZED VIEW IF EXISTS mv_vgeb_resolved;

CREATE MATERIALIZED VIEW mv_vgeb_resolved AS
SELECT
  -- Basis (ohne ind/stat/evk/amt*/handlungs*)
  v.x_recno                               AS vgeb_id,
  v.x_urk                                 AS urk,
  v.x_dat_kz                              AS dat_kz,
  v.x_udatfr                              AS udatfr,
  v.x_udatum                              AS udatum,

  -- Geburt
  v.g_geb_dfr                             AS geb_dfr,
  v.g_geb_d                               AS geb_datum,
  v.g_geb_std                             AS geb_stunde,

  -- Kind (ohne Verwgr)
  k.x_recno                               AS kind_id,
  k.p_name                                AS kind_name,
  k.p_vname                               AS kind_vorname,
  k.p_sex                                 AS kind_sex,
  k.p_geb_dat                             AS kind_geb_datum,

  -- Vater (ohne Verwgr, ohne sex)
  vt.x_recno                              AS vater_id,
  vt.p_name                               AS vater_name,
  vt.p_vname                              AS vater_vorname,

  -- Mutter (ohne Verwgr, ohne sex)
  mu.x_recno                              AS mutter_id,
  mu.p_name                               AS mutter_name,
  mu.p_vname                              AS mutter_vorname,

  -- Teilnehmer-Slots: Name/Vorname + Verwgr-Text
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

FROM vgeb v
LEFT JOIN vper k    ON k.x_recno   = v.g_kind
LEFT JOIN vper vt   ON vt.x_recno  = v.g_v_kd
LEFT JOIN vper mu   ON mu.x_recno  = v.g_m_kd

-- Teilnehmer + Verwgr-Text
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

-- Eindeutiger Schlüssel (für CONCURRENTLY)
CREATE UNIQUE INDEX IF NOT EXISTS mv_vgeb_resolved_uq
  ON mv_vgeb_resolved (vgeb_id);

-- Optionale Filter-Indizes (z. B. auf Datum)
CREATE INDEX IF NOT EXISTS mv_vgeb_resolved_datum_idx ON mv_vgeb_resolved (geb_datum);

-- Erstbefüllung
REFRESH MATERIALIZED VIEW mv_vgeb_resolved;
-- Später ohne Sperre:
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_vgeb_resolved;
