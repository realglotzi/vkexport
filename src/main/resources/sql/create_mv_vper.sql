DROP MATERIALIZED VIEW IF EXISTS mv_vper_resolved;

CREATE MATERIALIZED VIEW mv_vper_resolved AS
SELECT
  -- Basis
  v.x_recno               AS person_id,
  v.p_name                AS name,
  v.p_vname               AS vorname,
  v.p_sex                 AS geschlecht,

  -- Verwandtschaftsgruppe
  w.v_vgr                 AS verwgr,

  -- Geburtsangaben
  v.p_geb_dat             AS geburts_datum,
  ortg.o_ort              AS geburts_ort_name,

  -- Taufangaben
  v.p_tau_dat             AS tauf_datum,
  ortt.o_ort              AS tauf_ort_name,

  -- Sterbeangaben
  v.p_ste_dat             AS sterbe_datum,
  orts.o_ort              AS sterbe_ort_name,

  -- Begräbnisangaben
  v.p_beg_dat             AS begraebnis_datum,
  ortb.o_ort              AS begraebnis_ort_name,

  -- Bemerkung
  v.x_bem                 AS bem

FROM vper v
LEFT JOIN vvwg w    ON w.x_recno   = v.p_verwgr
LEFT JOIN vort ortg ON ortg.x_recno = v.p_geb_ort
LEFT JOIN vort ortt ON ortt.x_recno = v.p_tau_ort
LEFT JOIN vort orts ON orts.x_recno = v.p_ste_ort
LEFT JOIN vort ortb ON ortb.x_recno = v.p_beg_ort
WITH NO DATA;

-- Index für CONCURRENTLY Refresh
CREATE UNIQUE INDEX IF NOT EXISTS mv_vper_resolved_uq
  ON mv_vper_resolved (person_id);

-- Erstbefüllung
REFRESH MATERIALIZED VIEW mv_vper_resolved;
