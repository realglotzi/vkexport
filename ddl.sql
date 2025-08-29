CREATE TABLE mb.vgeb (
  x_recno int4 PRIMARY KEY,
  x_ind varchar(1),
  x_stand date,
  x_vamt int4,
  x_urk varchar(11),
  x_stat varchar(1),
  x_dat_kz varchar(1),
  x_udatfr varchar(12),
  x_udatum varchar(10),
  g_evk int4,
  g_geb_dfr varchar(12),
  g_geb_d varchar(10),
  g_geb_std varchar(5),
  g_kind int4,
  g_v_kd int4,
  g_m_kd int4,
  g_h_dat varchar(10),
  g_h_ort int4,
  x_pers1 int4,
  x_pers2 int4,
  x_pers3 int4,
  x_pers4 int4,
  x_pers5 int4,
  x_pers6 int4,
  x_pers7 int4,
  x_pers8 int4,
  x_pers9 int4,
  x_persa int4,
  x_persb int4,
  x_persc int4,
  x_bem text
);

CREATE TABLE mb.vhei (
  x_recno int4 PRIMARY KEY,
  x_ind varchar(1),
  x_stand date,
  x_vamt int4,
  x_urk varchar(11),
  x_stat varchar(1),
  x_dat_kz varchar(1),
  x_udatfr varchar(12),
  x_udatum varchar(10),
  h_ebg int4,
  h_evbg int4,
  h_evbt int4,
  h_hei_dfr varchar(12),
  h_hei_d varchar(10),
  h_hei_ort int4,
  h_bg int4,
  h_bg_v int4,
  h_bg_m int4,
  h_bt int4,
  h_bt_v int4,
  h_bt_m int4,
  x_pers1 int4,
  x_pers2 int4,
  x_pers3 int4,
  x_pers4 int4,
  x_pers5 int4,
  x_pers6 int4,
  x_pers7 int4,
  x_pers8 int4,
  x_pers9 int4,
  x_persa int4,
  x_persb int4,
  x_persc int4,
  x_bem text
);

CREATE TABLE mb.vste (
  x_recno int4 PRIMARY KEY,
  x_ind varchar(1),
  x_stand date,
  x_vamt int4,
  x_urk varchar(11),
  x_stat varchar(1),
  x_dat_kz varchar(1),
  x_udatfr varchar(12),
  x_udatum varchar(10),
  s_evst int4,
  s_evvst int4,
  s_ste_dfr varchar(12),
  s_ste_d varchar(10),
  s_ste_std varchar(5),
  s_beg_d varchar(10),
  s_beg_ort int4,
  s_vst int4,
  s_vst_ep int4,
  s_vst_v int4,
  s_vst_m int4,
  s_t_urs varchar(28),
  x_pers1 int4,
  x_pers2 int4,
  x_pers3 int4,
  x_pers4 int4,
  x_pers5 int4,
  x_pers6 int4,
  x_pers7 int4,
  x_pers8 int4,
  x_pers9 int4,
  x_persa int4,
  x_persb int4,
  x_persc int4,
  x_bem text
);

CREATE TABLE mb.vper (
  x_recno int4 PRIMARY KEY,
  x_ind varchar(1),
  x_stand date,
  x_zurk int4,
  x_stat varchar(1),
  p_reg varchar(1),
  p_event_d varchar(10),
  p_eltern int4,
  p_ehe int4,
  p_bruder int4,
  p_ket1 int4,
  p_ketn int4,
  p_aper int4,
  p_stpers int4,
  p_verwgr int4,
  p_sex varchar(1),
  p_name varchar(30),
  p_vname varchar(33),
  p_beruf varchar(30),
  p_ledig varchar(9),
  p_konf varchar(2),
  p_geb_dat varchar(10),
  p_geb_d_s varchar(1),
  p_geb_ort int4,
  p_tau_dat varchar(10),
  p_tau_ort int4,
  p_ste_dat varchar(10),
  p_ste_ort int4,
  p_beg_dat varchar(10),
  p_beg_ort int4,
  p_wohnort int4,
  p_her_ort int4,
  p_alter_j varchar(3),
  p_alter_m varchar(2),
  p_alter_w varchar(2),
  p_alter_t varchar(2),
  x_bem text
);

CREATE TABLE mb.vamt (
  x_recno int4 PRIMARY KEY,
  x_ind varchar(1),
  a_kuerz varchar(5),
  a_konf varchar(2),
  a_ort varchar(20),
  a_amt varchar(20),
  a_ao varchar(2),
  a_aa varchar(2)
);

CREATE TABLE mb.vfam (
  x_recno int4 PRIMARY KEY,
  x_ind varchar(1),
  x_stat varchar(1),
  f_vater int4,
  f_mutter int4,
  f_kind int4,
  f_mfg1 int4,
  f_mfgn int4,
  f_vatweh1 int4,
  f_vatwehe int4,
  f_mutweh1 int4,
  f_mutwehe int4,
  f_ket1 int4,
  f_ketn int4,
  f_vmeh1 int4,
  f_vmehn int4,
  f_mmeh1 int4,
  f_mmehn int4,
  f_afam varchar(3)
);

CREATE TABLE mb.vort (
  x_recno int4 PRIMARY KEY,
  x_ind varchar(1),
  x_stat varchar(1),
  o_ort varchar(20),
  o_erg varchar(35),
  o_aort varchar(2)
);

CREATE TABLE mb.vvwg (
  x_recno int4 PRIMARY KEY,
  x_ind varchar(1),
  x_stat varchar(1),
  v_vgr varchar(8),
  v_text varchar(35)
);

ALTER TABLE mb.vgeb ADD CONSTRAINT fk_vgeb_x_vamt FOREIGN KEY (x_vamt) REFERENCES mb.vamt(x_recno);
ALTER TABLE mb.vgeb ADD CONSTRAINT fk_vgeb_g_kind FOREIGN KEY (g_kind) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vgeb ADD CONSTRAINT fk_vgeb_g_v_kd FOREIGN KEY (g_v_kd) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vgeb ADD CONSTRAINT fk_vgeb_g_m_kd FOREIGN KEY (g_m_kd) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vgeb ADD CONSTRAINT fk_vgeb_g_h_ort FOREIGN KEY (g_h_ort) REFERENCES mb.vort(x_recno);
ALTER TABLE mb.vgeb ADD CONSTRAINT fk_vgeb_x_pers1 FOREIGN KEY (x_pers1) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vgeb ADD CONSTRAINT fk_vgeb_x_pers2 FOREIGN KEY (x_pers2) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vgeb ADD CONSTRAINT fk_vgeb_x_pers3 FOREIGN KEY (x_pers3) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vgeb ADD CONSTRAINT fk_vgeb_x_pers4 FOREIGN KEY (x_pers4) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vgeb ADD CONSTRAINT fk_vgeb_x_pers5 FOREIGN KEY (x_pers5) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vgeb ADD CONSTRAINT fk_vgeb_x_pers6 FOREIGN KEY (x_pers6) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vgeb ADD CONSTRAINT fk_vgeb_x_pers7 FOREIGN KEY (x_pers7) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vgeb ADD CONSTRAINT fk_vgeb_x_pers8 FOREIGN KEY (x_pers8) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vgeb ADD CONSTRAINT fk_vgeb_x_pers9 FOREIGN KEY (x_pers9) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vgeb ADD CONSTRAINT fk_vgeb_x_persa FOREIGN KEY (x_persa) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vgeb ADD CONSTRAINT fk_vgeb_x_persb FOREIGN KEY (x_persb) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vgeb ADD CONSTRAINT fk_vgeb_x_persc FOREIGN KEY (x_persc) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vhei ADD CONSTRAINT fk_vhei_x_vamt FOREIGN KEY (x_vamt) REFERENCES mb.vamt(x_recno);
ALTER TABLE mb.vhei ADD CONSTRAINT fk_vhei_h_ebg FOREIGN KEY (h_ebg) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vhei ADD CONSTRAINT fk_vhei_h_evbg FOREIGN KEY (h_evbg) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vhei ADD CONSTRAINT fk_vhei_h_hei_ort FOREIGN KEY (h_hei_ort) REFERENCES mb.vort(x_recno);
ALTER TABLE mb.vhei ADD CONSTRAINT fk_vhei_h_bg FOREIGN KEY (h_bg) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vhei ADD CONSTRAINT fk_vhei_h_bg_v FOREIGN KEY (h_bg_v) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vhei ADD CONSTRAINT fk_vhei_h_bg_m FOREIGN KEY (h_bg_m) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vhei ADD CONSTRAINT fk_vhei_h_bt FOREIGN KEY (h_bt) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vhei ADD CONSTRAINT fk_vhei_h_bt_v FOREIGN KEY (h_bt_v) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vhei ADD CONSTRAINT fk_vhei_h_bt_m FOREIGN KEY (h_bt_m) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vhei ADD CONSTRAINT fk_vhei_x_pers1 FOREIGN KEY (x_pers1) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vhei ADD CONSTRAINT fk_vhei_x_pers2 FOREIGN KEY (x_pers2) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vhei ADD CONSTRAINT fk_vhei_x_pers3 FOREIGN KEY (x_pers3) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vhei ADD CONSTRAINT fk_vhei_x_pers4 FOREIGN KEY (x_pers4) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vhei ADD CONSTRAINT fk_vhei_x_pers5 FOREIGN KEY (x_pers5) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vhei ADD CONSTRAINT fk_vhei_x_pers6 FOREIGN KEY (x_pers6) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vhei ADD CONSTRAINT fk_vhei_x_pers7 FOREIGN KEY (x_pers7) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vhei ADD CONSTRAINT fk_vhei_x_pers8 FOREIGN KEY (x_pers8) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vhei ADD CONSTRAINT fk_vhei_x_pers9 FOREIGN KEY (x_pers9) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vhei ADD CONSTRAINT fk_vhei_x_persa FOREIGN KEY (x_persa) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vhei ADD CONSTRAINT fk_vhei_x_persb FOREIGN KEY (x_persb) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vhei ADD CONSTRAINT fk_vhei_x_persc FOREIGN KEY (x_persc) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vste ADD CONSTRAINT fk_vste_x_vamt FOREIGN KEY (x_vamt) REFERENCES mb.vamt(x_recno);
ALTER TABLE mb.vste ADD CONSTRAINT fk_vste_s_beg_ort FOREIGN KEY (s_beg_ort) REFERENCES mb.vort(x_recno);
ALTER TABLE mb.vste ADD CONSTRAINT fk_vste_s_vst FOREIGN KEY (s_vst) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vste ADD CONSTRAINT fk_vste_s_vst_ep FOREIGN KEY (s_vst_ep) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vste ADD CONSTRAINT fk_vste_s_vst_v FOREIGN KEY (s_vst_v) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vste ADD CONSTRAINT fk_vste_s_vst_m FOREIGN KEY (s_vst_m) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vste ADD CONSTRAINT fk_vste_x_pers1 FOREIGN KEY (x_pers1) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vste ADD CONSTRAINT fk_vste_x_pers2 FOREIGN KEY (x_pers2) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vste ADD CONSTRAINT fk_vste_x_pers3 FOREIGN KEY (x_pers3) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vste ADD CONSTRAINT fk_vste_x_pers4 FOREIGN KEY (x_pers4) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vste ADD CONSTRAINT fk_vste_x_pers5 FOREIGN KEY (x_pers5) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vste ADD CONSTRAINT fk_vste_x_pers6 FOREIGN KEY (x_pers6) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vste ADD CONSTRAINT fk_vste_x_pers7 FOREIGN KEY (x_pers7) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vste ADD CONSTRAINT fk_vste_x_pers8 FOREIGN KEY (x_pers8) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vste ADD CONSTRAINT fk_vste_x_pers9 FOREIGN KEY (x_pers9) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vste ADD CONSTRAINT fk_vste_x_persa FOREIGN KEY (x_persa) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vste ADD CONSTRAINT fk_vste_x_persb FOREIGN KEY (x_persb) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vste ADD CONSTRAINT fk_vste_x_persc FOREIGN KEY (x_persc) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vper ADD CONSTRAINT fk_vper_p_eltern FOREIGN KEY (p_eltern) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vper ADD CONSTRAINT fk_vper_p_ehe FOREIGN KEY (p_ehe) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vper ADD CONSTRAINT fk_vper_p_bruder FOREIGN KEY (p_bruder) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vper ADD CONSTRAINT fk_vper_p_ket1 FOREIGN KEY (p_ket1) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vper ADD CONSTRAINT fk_vper_p_ketn FOREIGN KEY (p_ketn) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vper ADD CONSTRAINT fk_vper_p_verwgr FOREIGN KEY (p_verwgr) REFERENCES mb.vvwg(x_recno);
ALTER TABLE mb.vper ADD CONSTRAINT fk_vper_p_geb_ort FOREIGN KEY (p_geb_ort) REFERENCES mb.vort(x_recno);
ALTER TABLE mb.vper ADD CONSTRAINT fk_vper_p_tau_ort FOREIGN KEY (p_tau_ort) REFERENCES mb.vort(x_recno);
ALTER TABLE mb.vper ADD CONSTRAINT fk_vper_p_ste_ort FOREIGN KEY (p_ste_ort) REFERENCES mb.vort(x_recno);
ALTER TABLE mb.vper ADD CONSTRAINT fk_vper_p_beg_ort FOREIGN KEY (p_beg_ort) REFERENCES mb.vort(x_recno);
ALTER TABLE mb.vper ADD CONSTRAINT fk_vper_p_her_ort FOREIGN KEY (p_her_ort) REFERENCES mb.vort(x_recno);
ALTER TABLE mb.vfam ADD CONSTRAINT fk_vfam_f_vater FOREIGN KEY (f_vater) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vfam ADD CONSTRAINT fk_vfam_f_mutter FOREIGN KEY (f_mutter) REFERENCES mb.vper(x_recno);
ALTER TABLE mb.vfam ADD CONSTRAINT fk_vfam_f_kind FOREIGN KEY (f_kind) REFERENCES mb.vper(x_recno);
