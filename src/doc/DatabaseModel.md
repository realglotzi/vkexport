# Datenbankmodell (Export-Zielschema)

Dieses Dokument beschreibt, wie die DBF-Strukturen in SQL überführt werden und welche **Schlüssel/Beziehungen** zu erwarten sind. 
Die exakten Spalten je Tabelle werden **automatisch** aus den DBF-Headern generiert (siehe `export.sql`).

## Gemeinsame Regeln

- **Primärschlüssel**: Jede Tabelle erhält `x_recno int4 PRIMARY KEY` (laufende Nr. während des Exports).
- **Typen-Mapping** (aus DBF-Feldtypen):
  - `C`/`V` → `varchar(length)`
  - `N`/`F` → `int4`/`int8` (falls `decimalCount==0`, abhängig von `length`), sonst `numeric(p,s)`
  - `I` → `int4`
  - `Y` → `numeric(19,4)`
  - `B`/`O` → `double precision`
  - `D` → `varchar(8)` (Format `yyyyMMdd`)
  - `T` → `timestamp`
  - `L` → `boolean`
  - `M` → `text` (Inhalt aus `.DBT`, Pointerlogik)
  - `G`/`P`/`Q` → `bytea`
- **Memo-Felder (M)**: Text wird per **DBT‑Pointer** (10 ASCII‑Ziffern oder 4‑Byte LE) geladen.
- **Zeichensatz**: konfigurierbar via `--cp` (Standard `Cp850`).

## Tabellenüberblick

Die folgenden Tabellen sind im Tool fest verdrahtet:
- `vgeb`, `vhei`, `vste`, `vper`, `vamt`, `vfam`, `vort`, `vvwg`

> Die genaue Spaltenliste siehst du nach dem Lauf im erzeugten `export.sql` unter den jeweiligen `CREATE TABLE`‑Statements.

## Schlüssel- und Fremdschlüssel-Logik (aus Feldnamen abgeleitet)

In den Quelldaten werden referenzielle IDs oft **als 2–3‑stellige Clipper‑Codes** gespeichert. 
Diese werden per **Base‑222** auf `INTEGER` abgebildet. Dadurch entstehen „natürliche“ Schlüssel, die du für **Joins** nutzen kannst.
Typische Felder:

- **Amt / Verwaltung**: `X_VAMT`, `G_EVK`
- **Ort**: `G_H_ORT`, `VVG_ORT`, `X_ORT`, `H_HEI_ORT`
- **Personen**: `X_PERS*`, `VVG_PER`
- **Kind/Elternschlüssel**: `G_KIND`, `G_V_KD`, `G_M_KD`, `F_KIND`, `F_V_KD`, `F_M_KD`
- **Bescheide/Hintergründe**: `H_BG`, `H_BG_V`, `H_BG_M`, `H_BT`, `H_BT_V`, `H_BT_M`
- **Verstorbene / Status** (in VSTE): `S_VST`, `S_VST_EP`, `S_VST_V`, `S_VST_M`

> Nach der Konvertierung kannst du diese Integer‑IDs als **FK‑Kandidaten** interpretieren. 
> Echte `FOREIGN KEY`‑Constraints werden **nicht** automatisch gesetzt (fehlende globale Kataloge).

## Bekannte Memo-Felder

- `X_BEM` (mehrere Tabellen, z. B. `vgeb`, `vhei`, `vste`) — Bemerkungen/Notizen, aus `.DBT` gelesen.
- Weitere `M`‑Felder werden automatisch erkannt und analog behandelt.

## Beispiel: Interpretation und Joins

```sql
-- Beispiel-Join: Person (vper) ↔ Gemeinde (vgeb) über konvertierte Schlüssel
SELECT p.x_recno   AS per_id,
       p.x_vamt    AS vamt_id,
       g.g_h_ort   AS ort_id,
       g.x_bem     AS bemerkung
FROM   mb.vper p
JOIN   mb.vgeb g ON g.x_vamt = p.x_vamt;
```

> Hinweis: Feldnamen können je nach Quelle variieren; bitte `export.sql` als maßgeblich ansehen.

## Unterschiede / Besonderheiten

- Einige `X_PERS*`‑Felder existieren mehrfach (`X_PERS0` … `X_PERS9` sowie `X_PERSA` … `X_PERSZ`). 
  Sie werden jeweils **einzeln** konvertiert.
- `H_HEI_ORT` wird **zwei‑stellig** interpretiert → nutzt `convert2RecnoToLong`.

## Erweiterungen

- **Blockgröße dynamisch** aus `.DBT`‑Header auslesen (falls ≠ 512).
- **Längere Memos**: Kettete Lesen über mehr als 16 Blöcke.
- **FK‑Erkennung**: Automatische FK‑Vorschläge auf Basis von Namensmustern und Wertedomänen.
- **Validierung**: Konsistenzchecks (z. B. existiert jede ID im Ziel?).

---

*Stand entspricht dem Build `1.2.0` (java‑vkexport‑memo8).*
