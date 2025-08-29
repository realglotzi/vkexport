# vkexport-full (Java) — DBF → SQL Exporter

Dieses Tool exportiert Clipper/dBASE-Tabellen (`.DBF` + `.DBT`) in ausführbare **SQL-Skripte** (DDL + INSERTs).
Es ist auf dein Projekt zugeschnitten: **globale Memo-Pointer-Logik** (DBT) und **Base‑222‑Konvertierungen**.

## Features

- **Memo-Felder zuverlässig**: Liest für *alle* `M`-Felder den **10‑Byte-Zeiger** direkt aus dem DBF-Datensatz   und holt den Text **exakt** aus der dazugehörigen `.DBT`-Datei. Ende bei `0x1A`, NUL‑Padding entfernt.
- **Base‑222-Konvertierung** (Clipper): 2–3‑stellige Schlüsselfelder (z. B. `X_PERS*`, `H_BG*`, `H_BT*`, `S_VST*`, `X_VAMT`, …)   werden nach `int` umgerechnet. Overrides pro Tabelle/Feld + Heuristik nach Namensmustern.
- Typprüfung überall über **`DBFField.getType().getCharCode()`**.
- Erzeugt vollständiges `export.sql`: `DROP/CREATE SCHEMA`, `CREATE TABLE …`, danach `INSERT …`.

## Build

```bash
mvn -q -DskipTests package
```

Das erzeugt ein Fat‑JAR unter:
```
target/vkexport-full-1.2.0-shaded.jar
```

## Run

```bash
java -jar target/vkexport-full-1.2.0-shaded.jar   --pfad t/           	# Ordner mit DBF/DBT
  --prefix LU         	# Dateipräfix, z. B. LU_VGEB.DBF
  --schema mb         	# Zielschema in SQL
  --out export.sql    	# Ausgabedatei
  --cp Cp850          	# Codepage (Standard: Cp850)
```

**Beispiel** (wie genutzt):
```bash
java -jar target/vkexport-full-1.2.0-shaded.jar --pfad t/ --prefix LU --schema mb --out export.sql --cp Cp850
```

## Argumente

| Flag       | Beschreibung                                                                 | Default |
|------------|------------------------------------------------------------------------------|---------|
| `--pfad`   | Pfad zum Verzeichnis mit `PREFIX_TAB.DBF`/`.DBT`                             | —       |
| `--prefix` | Dateipräfix (z. B. `LU` → `LU_VGEB.DBF`)                                      | —       |
| `--schema` | Ziel-Schema-Name im SQL                                                      | —       |
| `--out`    | Ausgabedatei                                                                 | `export.sql` |
| `--cp`     | Zeichenkodierung (Java-Charset), z. B. `Cp850`, `Windows-1252`, `UTF-8`      | `Cp850` |
| `--mapfile`| (Optional) Properties: `TABLE.FIELD=conv|conv2|none` für Overrides           | —       |

### Memo-Handling (DBT)

- Zeigerformat: 10 ASCII‑Ziffern (klassisch) **oder** 4‑Byte little‑endian Integer im Feld.
- Blockgröße: standardmäßig **512** Bytes (Clipper/dBASE). Bis zu 16 Blöcke werden gelesen.
- Stop-Kriterien: erstes `0x1A` (EOF) oder Dateiende; trailing `0x1A`/`\0` werden entfernt; `CRLF` → `LF`.

> Wenn deine `.DBT` eine andere Blockgröße hat oder Memos länger sind: Sag kurz Bescheid — dann erweitern wir auf **Header‑basierte Blockgrößenerkennung** und **Blockketten**.

### Base‑222‑Konvertierung

- 3‑stellige Codes: `((c0-32)*222 + (c1-32))*222 + (c2-32)`
- 2‑stellige Codes: `(c0-32)*222 + (c1-32)`
- Gilt für in `TABLE_FIELD_OVERRIDES` gelistete Felder und für per Heuristik erkannte `CHAR(2|3)`‑Felder mit passenden Namen.
- `conv` = 2/3‑stellig, `conv2` = **nur** 2‑stellig (z. B. `H_HEI_ORT`).

Mehr Details siehe **docs/DatabaseModel.md**.

## Output

- `export.sql` enthält:
  1. `DROP SCHEMA <schema> CASCADE;`
  2. `CREATE SCHEMA <schema>;`
  3. Für jede Tabelle: `CREATE TABLE …` (inkl. `x_recno int4 PRIMARY KEY` + Feldtypen-Mapping)
  4. Alle Datensätze als `INSERT …`

## Troubleshooting

- **Memo-Feld leer/zu lang** → Prüfe `.DBT` neben `.DBF`, Dateiname/Präfix korrekt?   Abweichende Blockgröße? → bitte melden.
- **Umlaute falsch** → `--cp` anpassen (z. B. `Windows-1252`). 
- **Feld fehlt** → Sind DBF/DBT-Dateien schreibfähig/lesbar? Hat die Tabelle den erwarteten Präfix?

## Lizenz

Projektbeispiel – nutzbar in deinem Projekt. Keine weiteren Einschränkungen.
