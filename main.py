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

START_DATE = "2026-01-22"
END_DATE   = "2026-02-18"
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

# Columnas “horas” que vamos a “vaciar” en el día anterior cuando movemos
HORAS_A_VACIAR_DIA_ANTERIOR = [
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

    user_map, turno_map, employee_ids = {}, {}, []

    for u in users:
        if u.get("status") != "ACTIVE":
            continue

        emp = u.get("employeeInternalId")
        if not emp:
            continue

        employee_ids.append(emp)
        user_map[emp] = f"{u.get('lastName','')}, {u.get('firstName','')}"

        turno = ""
        for seg in (u.get("segmentations") or []):
            if (seg.get("group") or "").strip() == "Turno":
                turno = seg.get("item") or ""
                break
        turno_map[emp] = turno

    return employee_ids, user_map, turno_map

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
    if real_start is None or pd.isna(real_start) or real_end is None or pd.isna(real_end):
        return "No"
    try:
        return "Si" if real_start.date() != real_end.date() else "No"
    except Exception:
        return "No"

# ================= DAY SUMMARIES =================
def fetch_batch(emp_ids, user_map, turno_map):
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
            worked_api = float(hours_obj.get("worked") or 0)

            # Horario obligatorio
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
                "_worked_api": worked_api
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
            ex.submit(fetch_batch, employee_ids[i:i + BATCH_SIZE], user_map, turno_map)
            for i in range(0, len(employee_ids), BATCH_SIZE)
        ]
        for f in as_completed(futures):
            rows.extend(f.result())

    df = pd.DataFrame(rows)

    for cat in CATEGORIAS:
        col = f"HORAS_{cat}"
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # Métrica base (la usás para mostrar, pero OJO: no la usamos para mover)

    return df

# ================= AJUSTE: NORMAL -> FERIADO (CRUCE) =================
def aplicar_ajuste_cruce_a_feriado(df_export: pd.DataFrame) -> pd.DataFrame:
    """
    TU REGLA:
    - Día anterior NO feriado
    - Cruce de día = Si
    - Día actual SÍ feriado
    => el turno nocturno (22-06) se imputa al día actual (feriado),
       sumando SOLO en HORAS_FERIADO del día actual.
    => el día anterior queda en 0 (para no duplicar).

    moved = horas reales del turno (por fichadas) = columna auxiliar _worked
    """
    df = df_export.copy()

    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    df = df.sort_values(["ID", "Fecha"]).reset_index(drop=True)

    df["Ajuste cruce→feriado"] = "No"

    def _is_si(v):
        return str(v).strip().lower() in ("si", "sí")

    def _num(v) -> float:
        try:
            if v is None or pd.isna(v):
                return 0.0
            return float(v)
        except Exception:
            return 0.0

    # asegurar columnas
    if "HORAS_FERIADO" not in df.columns:
        df["HORAS_FERIADO"] = 0.0
    if "Horas Trabajadas" not in df.columns:
        df["Horas Trabajadas"] = 0.0
    if "_worked" not in df.columns:
        df["_worked"] = 0.0

    # asegurar columnas a vaciar
    for c in HORAS_A_VACIAR_DIA_ANTERIOR:
        if c not in df.columns:
            df[c] = 0.0

    for uid, idxs in df.groupby("ID").groups.items():
        idxs = list(idxs)
        for j in range(1, len(idxs)):
            i_prev = idxs[j - 1]
            i_cur  = idxs[j]

            cur_fer    = _is_si(df.at[i_cur,  "Es Feriado"])
            prev_fer   = _is_si(df.at[i_prev, "Es Feriado"])
            prev_cruce = _is_si(df.at[i_prev, "Cruce de día"])

            # SOLO normal -> feriado con cruce
            if not (cur_fer and prev_cruce and not prev_fer):
                continue

            moved = _num(df.at[i_prev, "_worked_api"])
            if moved <= 0:
                continue

            # ✅ acumular SOLO en HORAS_FERIADO del feriado
            df.at[i_cur, "HORAS_FERIADO"] = _num(df.at[i_cur, "HORAS_FERIADO"]) + moved

            # ✅ para que "Horas Trabajadas" del 16 muestre las 2 acumuladas (si la columna la usás)
            df.at[i_cur, "Horas Trabajadas"] = _num(df.at[i_cur, "Horas Trabajadas"]) + moved

            # ✅ día anterior en 0 (para que no duplique)
            for c in HORAS_A_VACIAR_DIA_ANTERIOR:
                df.at[i_prev, c] = 0.0

            df.at[i_cur, "Ajuste cruce→feriado"] = "Si"

            if "Observaciones" in df.columns:
                obs = str(df.at[i_cur, "Observaciones"] or "").strip()
                tag = f"Ajuste cruce→feriado desde {df.at[i_prev,'Fecha'].date()}"
                df.at[i_cur, "Observaciones"] = (obs + " | " + tag).strip(" |")

    # redondeo
    for c in ["HORAS_FERIADO", "Horas Trabajadas"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0).round(2)

    return df

# ================= MAIN =================
def main():
    employee_ids, user_map, turno_map = fetch_users()
    print(f"Usuarios ACTIVE: {len(employee_ids)}")

    df = build_df(employee_ids, user_map, turno_map)
    df = df.sort_values(by=["ID", "FECHA"], ascending=[True, True]).reset_index(drop=True)

    df_export = df.copy()

    df_export["Horas Trabajadas"] = pd.to_numeric(
        df_export["_worked_api"], errors="coerce"
    ).fillna(0.0).round(2)

    df_export["TARDANZA"] = df_export.apply(
        lambda r: round(max(0.0, calc_delta_hours(r["_rs"], r["_ss"], TOLERANCIA_TARDANZA_SEG)), 2),
        axis=1
    )

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
# 🔵 Si tiene HORAS_FERIADO, forzar extras a cero
    df_export = forzar_extras_a_cero_si_feriado(df_export)
    # ✅ aplicar ajuste ANTES de dropear _rs/_re (los necesitamos para _worked)
    df_export = aplicar_ajuste_cruce_a_feriado(df_export)
    df_export = forzar_extras_a_cero_si_feriado_o_franco(df_export)
    # ahora sí, dropeo internos
    df_export["Horas_Netas"] = (
        df_export["HORAS_EXTRA"] - df_export["LLEGADA_ANTICIPADA"]
    ).round(2)

    #df_export = restar_llegada_anticipada(df_export)

    df_export = df_export.drop(columns=["_ss","_se","_rs","_re","_worked","_worked_api"], errors="ignore")

    cols_final = [
        "ID",
        #"Apellido, Nombre",
        "Fecha",
        "dia", 
        "Turno",
        #"Ausencia",
        #"Tardanza -", 
        # "TARDANZA", 
        # "Trabajo Insuficiente",
        # "Es Feriado",
        # "Licencia",
        #"Cruce de día",
        # "Ajuste cruce→feriado",
        "Observaciones",
        "Horas_Netas",
        "HORAS_EXTRA",
        "LLEGADA_ANTICIPADA",

        "Horario obligatorio",
        "Fichadas", 
        "Horas planificadas",
        "Horas Trabajadas",
        "HORAS_FRANCO",
        "HORAS_FERIADO",
        #"HORAS_FERIADO NOCTURNA",
        # "HORAS_FRANCO NOCTURNA",

        #"HORAS_REGULAR",
    "HORAS_EXTRA AL 50","HORAS_EXTRA AL 100",#"HORAS_NOCTURNA",
    ]

    for c in cols_final:
        if c not in df_export.columns:
            df_export[c] = np.nan

    df_export = df_export[cols_final]
    df_export["Turno"] = df_export["Turno"].fillna("")
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

    df_export["Fecha"] = pd.to_datetime(df_export["Fecha"], errors="coerce").dt.strftime("%Y-%m-%d")

    export_detalle_diario_excel(
        df_export=df_export,
        out=out,
        START_DATE=START_DATE,
        END_DATE=END_DATE,
        generated_at=generated_at,
        EXPORTAR_DECIMAL=True,
        COLS_HORAS_DETALLE=[c for c in HORAS_A_VACIAR_DIA_ANTERIOR if c in df_export.columns],
    )
    """
    pintar_flags_si_no(
        path_xlsx=out,
        sheet_name="Detalle diario",
        cols_flag=[
            "Ausencia","Tardanza -","Trabajo Insuficiente","Es Feriado","Licencia",
            "Cruce de día","Ajuste cruce→feriado"
        ]
    )
     """
    #agregar_resumen_turnos(out)
    print("Excel generado:", out)

if __name__ == "__main__":
    main()