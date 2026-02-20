import math
import requests
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from openpyxl import load_workbook
from openpyxl.styles import PatternFill

from aux_functions import *

# ================= CONFIG =================
BASE = "https://api-prod.humand.co/public/api/v1"
AUTH = "Basic NDY4NzQwMzpseHhBWGNzdGJDVERRWEpHTFg0SU41MzJfTVpNRENSdg=="

START_DATE = "2025-12-13"
END_DATE   = "2026-01-13"

LIMIT_USERS = 50
LIMIT_DAYS  = 500
BATCH_SIZE  = 25
MAX_WORKERS = 8

TZ_AR = ZoneInfo("America/Argentina/Buenos_Aires")
NORMALIZAR_A_MINUTO = False

TOLERANCIA_TARDANZA_SEG = 0

FLAG_COLS = ["Ausencia", "Tardanza -", "Trabajo Insuficiente", "Es Feriado", "Licencia"]

CATEGORIAS = [
    "REGULAR",
    "NOCTURNA",
    "EXTRA",
    "EXTRA SABADO",
    "EXTRA AL 50",
    "EXTRA AL 100",
    "EXTRA DOMINGO",
    "FRANCO",
    "FERIADO",
    "FERIADO NOCTURNA",
    "FRANCO NOCTURNA",
    "NOCTURNA 2",
]

# Columnas “horas” que vamos a mover en el ajuste
HORAS_A_MOVER = [
    "Horas Trabajadas",
    "HORAS_REGULAR",
    "HORAS_EXTRA",
    "HORAS_EXTRA AL 50",
    "HORAS_EXTRA AL 100",
    "HORAS_NOCTURNA",
    "HORAS_FRANCO",
    "HORAS_FERIADO",
    "HORAS_FERIADO NOCTURNA",
    "HORAS_FRANCO NOCTURNA",
    "HORAS_NOCTURNA 2",
    "Extra Sábado",
    "Extra Domingo",
]

# ================= SESSION =================
s = requests.Session()
s.headers.update({"Authorization": AUTH})

def get(url, params):
    r = s.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

# ================= USERS =================
def fetch_users():
    first = get(f"{BASE}/users", {"page": 1, "limit": LIMIT_USERS})
    pages = math.ceil(first["count"] / LIMIT_USERS)

    users = first["users"]
    for p in range(2, pages + 1):
        users += get(f"{BASE}/users", {"page": p, "limit": LIMIT_USERS})["users"]

    user_map, legajo_map, esquema_map, turno_map, employee_ids = {}, {}, {}, {}, []

    for u in users:
        if u.get("status") != "ACTIVE":
            continue

        emp = u.get("employeeInternalId")
        if not emp:
            continue

        employee_ids.append(emp)

        # Nombre
        user_map[emp] = f"{u.get('lastName','')}, {u.get('firstName','')}"

        turno = ""
        for seg in (u.get("segmentations") or []):
            if (seg.get("group") or "").strip() == "Turno":
                turno = seg.get("item") or ""
                break
        turno_map[emp] = turno

    return employee_ids, user_map,  turno_map


# ================= CATEGORÍAS =================
def split_categorized_hours_basic(categorized_hours, categorias_validas):
    valid_upper = {c.upper(): c for c in categorias_validas}
    out = {f"HORAS_{c}": 0.0 for c in categorias_validas}

    for ch in categorized_hours or []:
        name = (ch.get("category", {}) or {}).get("name") or ""
        name_u = str(name).upper().strip()
        if name_u in valid_upper:
            label = valid_upper[name_u]
            out[f"HORAS_{label}"] += float(ch.get("hours") or 0)

    return {k: round(v, 2) for k, v in out.items()}

# ================= CRUCE DE DÍA =================
def calcular_cruce_dia(real_start, real_end) -> str:
    """
    Cruce de día = Si si hay fichadas y la fecha calendario de inicio != fin
    (en TZ_AR). Si falta uno de los dos: No.
    """
    if real_start is None or pd.isna(real_start) or real_end is None or pd.isna(real_end):
        return "No"
    try:
        return "Si" if real_start.date() != real_end.date() else "No"
    except Exception:
        return "No"

# ================= DAY SUMMARIES =================
def fetch_batch(emp_ids, user_map,  turno_map):
    rows = []
    page = 1

    while True:
        data = get(
            f"{BASE}/time-tracking/day-summaries",
            {
                "employeeIds": ",".join(emp_ids),
                "startDate": START_DATE,
                "endDate": END_DATE,
                "limit": LIMIT_DAYS,
                "page": page,
            },
        )

        items = data.get("items", [])
        if not items:
            break

        for it in items:
            emp = it.get("employeeId")
            ref = (it.get("referenceDate") or it.get("date") or "")[:10]
            if not ref:
                continue

            entries = it.get("entries") or []
            slots   = it.get("timeSlots") or []
            incid   = it.get("incidences") or []
            tor     = it.get("timeOffRequests") or []
            hol     = it.get("holidays") or []
            cat     = it.get("categorizedHours") or []

            flags = flags_incidencias_y_eventos(
                incidences=incid,
                time_off_requests=tor,
                holidays=hol
            )

            hours_obj = it.get("hours") or {}
            scheduled = float(hours_obj.get("scheduled") or 0)

            # Horario obligatorio (primer timeslot)
            sched_start = sched_end = pd.NaT
            if slots and isinstance(slots, list):
                d = datetime.strptime(ref, "%Y-%m-%d")
                s0 = slots[0] if slots else {}
                if isinstance(s0, dict):
                    if s0.get("startTime"):
                        try:
                            h, m = map(int, s0["startTime"].split(":"))
                            sched_start = datetime(d.year, d.month, d.day, h, m, tzinfo=TZ_AR)
                        except Exception:
                            sched_start = pd.NaT
                    if s0.get("endTime"):
                        try:
                            h, m = map(int, s0["endTime"].split(":"))
                            sched_end = datetime(d.year, d.month, d.day, h, m, tzinfo=TZ_AR)
                            if not pd.isna(sched_start) and sched_end < sched_start:
                                sched_end += timedelta(days=1)
                        except Exception:
                            sched_end = pd.NaT

            # Fichadas
            real_start = real_end = pd.NaT
            if entries and isinstance(entries, list):
                entries_sorted = sorted(
                    [e for e in entries if isinstance(e, dict) and (e.get("time") or e.get("date"))],
                    key=lambda e: iso_to_dt(e.get("time") or e.get("date"), TZ_AR)
                )

                starts_dt = [
                    iso_to_dt(e.get("time") or e.get("date"), TZ_AR)
                    for e in entries_sorted
                    if (e.get("type") or "").upper().strip() == "START"
                ]
                ends_dt = [
                    iso_to_dt(e.get("time") or e.get("date"), TZ_AR)
                    for e in entries_sorted
                    if (e.get("type") or "").upper().strip() == "END"
                ]
                if starts_dt:
                    real_start = min(starts_dt)
                if ends_dt:
                    real_end = max(ends_dt)

            if NORMALIZAR_A_MINUTO:
                sched_start = floor_minute(sched_start)
                sched_end   = floor_minute(sched_end)
                real_start  = floor_minute(real_start)
                real_end    = floor_minute(real_end)

            cat_hours = split_categorized_hours_basic(cat, CATEGORIAS)

            row = {
                "ID": emp,
                "APELLIDO, NOMBRE": user_map.get(emp, ""),
                "FECHA": ref,
                "DIA": weekday_es(ref),
                "Turno": turno_map.get(emp, ""),

                "Ausencia": flags["Ausencia"],
                "Tardanza -": flags["Tardanza"],
                "Trabajo Insuficiente": flags["Trabajo Insuficiente"],
                "Es Feriado": flags["Es Feriado"],
                "Licencia": flags["Licencia"],

                "_ss": sched_start,
                "_se": sched_end,
                "_rs": real_start,
                "_re": real_end,

                "Cruce de día": calcular_cruce_dia(real_start, real_end),

                "HORARIO_OBLIGATORIO": fmt_range(sched_start, sched_end),
                "FICHADAS": fmt_range(real_start, real_end),
                "OBSERVACIONES": build_observaciones(it),
                "PLANIFICADAS": scheduled,
            }

            row.update(cat_hours)
            rows.append(row)

        if len(items) < LIMIT_DAYS:
            break
        page += 1

    return rows

def build_df(employee_ids, user_map, turno_map):
    rows = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [
            ex.submit(
                fetch_batch,
                employee_ids[i:i + BATCH_SIZE],
                user_map,
                turno_map
            )
            for i in range(0, len(employee_ids), BATCH_SIZE)
        ]
        for f in as_completed(futures):
            rows.extend(f.result())

    df = pd.DataFrame(rows)

    # Normalizar categorías numéricas
    for cat in CATEGORIAS:
        col = f"HORAS_{cat}"
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # Métricas base
    df["HORAS_TRABAJADAS"] = (df["HORAS_REGULAR"] + df["HORAS_EXTRA"]).round(2)

    return df

# ================= AJUSTE: CRUCE -> FERIADO =================
def aplicar_ajuste_cruce_a_feriado(df_export: pd.DataFrame) -> pd.DataFrame:
    """
    Regla:
    - Si una fila es feriado (Es Feriado == 'Si')
    - y la fila anterior del MISMO usuario tiene Cruce de día == 'Si'
    - y la fila anterior NO es feriado
    => se “mueven” horas del día anterior al feriado:
       - se suman a Horas Trabajadas del feriado
       - y se suman a HORAS_EXTRA AL 100 del feriado (pago 100%)
       - y se limpian del día anterior
    """
    df = df_export.copy()

    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    df = df.sort_values(["ID", "Fecha"]).reset_index(drop=True)

    df["Ajuste cruce→feriado"] = "No"

    # asegurar columnas numéricas
    for c in HORAS_A_MOVER:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        else:
            df[c] = np.nan

    def _is_si(v):
        return str(v).strip().lower() in ("si", "sí")

    for uid, g_idx in df.groupby("ID").groups.items():
        idxs = list(g_idx)
        for j in range(1, len(idxs)):
            i_prev = idxs[j - 1]
            i_cur  = idxs[j]

            cur_fer = _is_si(df.at[i_cur, "Es Feriado"])
            prev_fer = _is_si(df.at[i_prev, "Es Feriado"])
            prev_cruce = _is_si(df.at[i_prev, "Cruce de día"])

            if not (cur_fer and prev_cruce and not prev_fer):
                continue

            # mover horas del día anterior (todo lo trabajado)
            moved = df.at[i_prev, "Horas Trabajadas"]
            if pd.isna(moved) or float(moved) <= 0:
                continue

            # 1) sumar a feriado
            df.at[i_cur, "Horas Trabajadas"] = (df.at[i_cur, "Horas Trabajadas"] or 0) + moved

            # 2) pagar 100%: sumar a Extra 100
            df.at[i_cur, "HORAS_EXTRA AL 100"] = (df.at[i_cur, "HORAS_EXTRA AL 100"] or 0) + moved

            # 3) reflejar en “HORAS_EXTRA” si la usás como “columna única”
            if "HORAS_EXTRA" in df.columns:
                df.at[i_cur, "HORAS_EXTRA"] = (df.at[i_cur, "HORAS_EXTRA"] or 0) + moved

            # 4) limpiar del día anterior (lo mínimo: trabajadas/regulares/extra/100/50/nocturnas)
            for c in ["Horas Trabajadas", "HORAS_REGULAR", "HORAS_EXTRA", "HORAS_EXTRA AL 50", "HORAS_EXTRA AL 100", "HORAS_NOCTURNA"]:
                if c in df.columns:
                    df.at[i_prev, c] = np.nan

            # 5) marcar ajuste + nota
            df.at[i_cur, "Ajuste cruce→feriado"] = "Si"
            obs = df.at[i_cur, "Observaciones"] if "Observaciones" in df.columns else ""
            prev_date = df.at[i_prev, "Fecha"]
            tag = f"Ajuste cruce→feriado desde {prev_date.date()}"
            df.at[i_cur, "Observaciones"] = (str(obs) + " | " + tag).strip(" |")

    # redondeo final, preservando NaN
    for c in ["Horas Trabajadas", "HORAS_EXTRA AL 100", "HORAS_EXTRA"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").round(2)

    return df

# ================= PINTAR AMARILLO EN EXCEL =================
def pintar_filas_amarillas_por_ajuste(path_xlsx: str, sheet_name: str = "Detalle diario"):
    wb = load_workbook(path_xlsx)
    ws = wb[sheet_name]

    # detectar fila de headers buscando “Ajuste cruce→feriado”
    header_row = None
    ajuste_col = None
    for r in range(1, 30):
        row_vals = [ws.cell(r, c).value for c in range(1, ws.max_column + 1)]
        if "Ajuste cruce→feriado" in row_vals:
            header_row = r
            ajuste_col = row_vals.index("Ajuste cruce→feriado") + 1
            break

    if header_row is None:
        wb.close()
        return

    fill_yellow = PatternFill("solid", fgColor="FFF2CC")  # amarillo suave

    for r in range(header_row + 1, ws.max_row + 1):
        v = ws.cell(r, ajuste_col).value
        if str(v).strip().lower() in ("si", "sí"):
            for c in range(1, ws.max_column + 1):
                ws.cell(r, c).fill = fill_yellow

    wb.save(path_xlsx)
    wb.close()

# ================= MAIN =================
def main():
    employee_ids, user_map, turno_map = fetch_users()

    print(f"Usuarios ACTIVE: {len(employee_ids)}")

    df = build_df(employee_ids, user_map, turno_map)

    # ordenar
    df = df.sort_values(by=["ID", "FECHA"], ascending=[True, True]).reset_index(drop=True)

    # ===== export base =====
    df_export = df.copy()
    df_export["TARDANZA"] = df_export.apply(
        lambda r: round(max(0.0, calc_delta_hours(r["_rs"], r["_ss"], TOLERANCIA_TARDANZA_SEG)), 2),
        axis=1
    )
# ✅ Llegada anticipada (solo informativo, NO se resta)
    df_export["LLEGADA_ANTICIPADA"] = df_export.apply(
        lambda r: round(max(0.0, calc_early_arrival_hours(r["_rs"], r["_ss"])), 2),
        axis=1
    )

    rename_excel = {
        "ID": "ID",
        "APELLIDO, NOMBRE": "Apellido, Nombre",
        "FECHA": "Fecha",
        "DIA": "dia",
        "PLANIFICADAS": "Horas planificadas",
        "HORARIO_OBLIGATORIO": "Horario obligatorio",
        "FICHADAS": "Fichadas",
        "OBSERVACIONES": "Observaciones",
        "HORAS_TRABAJADAS": "Horas Trabajadas",

    }

    df_export = df_export.rename(columns=rename_excel)

    df_export = df_export.drop(columns=["_ss","_se","_rs","_re"], errors="ignore")

    # ===== aplicar ajuste cruce→feriado (ANTES de exportar) =====
    df_export = aplicar_ajuste_cruce_a_feriado(df_export)

    # ===== orden final =====
    cols_final = [
        "ID","Apellido, Nombre","Fecha","dia", "Turno",
        "Ausencia","Tardanza -", "TARDANZA", "Trabajo Insuficiente","Es Feriado","Licencia",
        "Cruce de día","Ajuste cruce→feriado",
        "Horario obligatorio","Fichadas", "LLEGADA_ANTICIPADA",
        "HORAS_FRANCO","HORAS_FERIADO","HORAS_FERIADO NOCTURNA","HORAS_FRANCO NOCTURNA","HORAS_NOCTURNA 2",
        "Observaciones",
        "Horas planificadas",
        "Horas Trabajadas","HORAS_REGULAR","HORAS_EXTRA","HORAS_EXTRA AL 50","HORAS_EXTRA AL 100","HORAS_NOCTURNA",
    ]


    for c in cols_final:
        if c not in df_export.columns:
            df_export[c] = np.nan

    df_export = df_export[cols_final]
    df_export["Turno"] = df_export["Turno"].fillna("")


    # ===== export =====
    now = datetime.now()
    out = now.strftime("%Y-%m-%d_%H-%M-%S") + "_reporte_basico.xlsx"
    generated_at = now.strftime("%Y-%m-%d %H:%M")
    COLS_CERO_VACIO = [
        "HORAS_FRANCO",
        "HORAS_FERIADO",
        "HORAS_FERIADO NOCTURNA",
        "HORAS_FRANCO NOCTURNA",
        "HORAS_NOCTURNA 2",
        "Extra Sábado",
        "Extra Domingo",
        "Horas planificadas",
        "Horas Trabajadas",
        "HORAS_REGULAR",
        "HORAS_EXTRA",
        "HORAS_EXTRA AL 50",
        "HORAS_EXTRA AL 100",
        "HORAS_NOCTURNA",
        "TARDANZA",
        "LLEGADA_ANTICIPADA"
    ]

    for c in COLS_CERO_VACIO:
        if c in df_export.columns:
            df_export[c] = df_export[c].where(df_export[c] != 0, np.nan)

    df_export["Fecha"] = df_export["Fecha"].dt.strftime("%Y-%m-%d")

    export_detalle_diario_excel(
        df_export=df_export,
        out=out,
        START_DATE=START_DATE,
        END_DATE=END_DATE,
        generated_at=generated_at,
        EXPORTAR_DECIMAL=True,
        COLS_HORAS_DETALLE=[c for c in HORAS_A_MOVER if c in df_export.columns],
    )

    # (si querés seguir coloreando flags como antes, dejalo)
    # colorear_flags_excel(path_xlsx=out, sheet_name="Detalle diario", flag_cols=FLAG_COLS)

    # ✅ pintar amarillo por ajuste

    
    pintar_flags_si_no(
        path_xlsx=out,
        sheet_name="Detalle diario",
        cols_flag=[
            "Ausencia","Tardanza -","Trabajo Insuficiente","Es Feriado","Licencia",
            "Cruce de día","Ajuste cruce→feriado"
        ]
    )
    agregar_resumen_turnos(out)


    print("Excel generado:", out)

if __name__ == "__main__":
    main()