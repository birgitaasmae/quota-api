from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional, Tuple
import math
import re
import httpx
import unicodedata

PXWEB_BASE = "https://andmed.stat.ee/api/v1/et/stat"
API_KEY = ""
DEFAULT_YEAR = 2025

ALLOWED_EDUCATION_IDS = {"2", "7", "11"}  # requested

CAPITAL_CITY = "Tallinn"
BIG_CITIES = {"Tartu", "Narva", "Pärnu", "Kohtla-Järve"}
REGION5 = {"Põhja-Eesti", "Kirde-Eesti", "Kesk-Eesti", "Lõuna-Eesti", "Lääne-Eesti"}


# ================= MODELS =================

class Reference(BaseModel):
    year: int = DEFAULT_YEAR

class AgeBand(BaseModel):
    from_age: int = Field(..., ge=0, alias="from")
    to_age: int = Field(..., ge=0, alias="to")

class QuotaRequest(BaseModel):
    reference: Reference = Reference()
    age_band: AgeBand
    sample_n: int = Field(..., gt=0)
    age_grouping_years: int = Field(10, ge=1, le=100)  # supports 1
    dimensions: List[str] = Field(default_factory=lambda: ["sex", "age_group"])

    sex_filter: str = Field(
        default="total",
        description="Sex filter: total | men | women (aliases: kokku/mehed/naised, m/f, male/female)."
    )

class QuotaCell(BaseModel):
    id: str
    label: str
    pop: int
    share: float
    quota: int

class DimensionResult(BaseModel):
    base: int
    cells: List[QuotaCell]
    notes: List[str] = []

class QuotaResponse(BaseModel):
    population_total: int
    sample_n: int
    results: Dict[str, DimensionResult]
    meta: Dict[str, Any]


# ================= HELPERS =================

def require_key(x_api_key: Optional[str]):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

def norm(s: str) -> str:
    s = str(s).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def fold(s: str) -> str:
    s = norm(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s).strip()
    return s

def clean_value_text(value_text: str) -> str:
    return str(value_text).lstrip(".").strip()

def largest_remainder(shares: List[float], n: int) -> List[int]:
    raw = [s * n for s in shares]
    floors = [int(math.floor(x)) for x in raw]
    remainder = n - sum(floors)
    fracs = sorted([(i, raw[i] - floors[i]) for i in range(len(shares))], key=lambda x: x[1], reverse=True)
    out = floors[:]
    for i in range(remainder):
        out[fracs[i][0]] += 1
    return out

def compute_cells(ids: List[str], labels: List[str], pops: List[int], n: int) -> Tuple[int, List[QuotaCell]]:
    base = sum(pops)
    shares = [(p / base) if base > 0 else 0.0 for p in pops]
    quotas = largest_remainder(shares, n)
    cells = [
        QuotaCell(id=str(ids[i]), label=str(labels[i]), pop=int(pops[i]), share=float(shares[i]), quota=int(quotas[i]))
        for i in range(len(shares))
    ]
    return base, cells

async def px_meta(table: str) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.get(f"{PXWEB_BASE}/{table}")
        r.raise_for_status()
        return r.json()

async def px_query(table: str, query: List[Dict[str, Any]]) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=120) as c:
        r = await c.post(
            f"{PXWEB_BASE}/{table}",
            json={"query": query, "response": {"format": "json-stat2"}},
        )
        r.raise_for_status()
        return r.json()

def parse_jsonstat(js: Dict[str, Any]):
    ds = js.get("dataset", js)
    ids = ds["id"]
    dim = ds["dimension"]
    size = ds["size"]
    vals = ds["value"]

    inv: Dict[str, List[str]] = {}
    labels: Dict[str, Dict[str, str]] = {}

    for d in ids:
        cat = dim[d]["category"]
        idx = cat["index"]
        inv_list = [None] * len(idx)
        for k, pos in idx.items():
            inv_list[pos] = k
        inv[d] = inv_list
        labels[d] = cat.get("label", {})

    mult = []
    prod = 1
    for s in reversed(size):
        mult.insert(0, prod)
        prod *= s

    rows = []
    for i, v in enumerate(vals):
        if v is None:
            continue
        coords = [(i // mult[j]) % size[j] for j in range(len(size))]
        rows.append((coords, int(v)))

    return ids, inv, labels, rows

def pick_var(variables: List[Dict[str, Any]], keywords: List[str]) -> Dict[str, Any]:
    kws_raw = [norm(k) for k in keywords]
    kws_fold = [fold(k) for k in keywords]

    for v in variables:
        t_raw = norm(v.get("text", ""))
        c_raw = norm(v.get("code", ""))
        t_f = fold(v.get("text", ""))
        c_f = fold(v.get("code", ""))

        if any(k == c_raw for k in kws_raw) or any(k == c_f for k in kws_fold):
            return v
        if any(k in t_raw for k in kws_raw) or any(k in c_raw for k in kws_raw):
            return v
        if any(k in t_f for k in kws_fold) or any(k in c_f for k in kws_fold):
            return v

    preview = [{"code": x.get("code"), "text": x.get("text")} for x in variables[:80]]
    raise HTTPException(status_code=500, detail={"msg": "Could not detect variable", "keywords": keywords, "variables_preview": preview})

def pick_var_exact(variables: List[Dict[str, Any]], exact_code_or_text: str) -> Dict[str, Any]:
    target = fold(exact_code_or_text)
    for v in variables:
        if fold(v.get("code", "")) == target or fold(v.get("text", "")) == target:
            return v
    preview = [{"code": x.get("code"), "text": x.get("text")} for x in variables[:80]]
    raise HTTPException(status_code=500, detail={"msg": "Could not detect exact variable", "exact": exact_code_or_text, "variables_preview": preview})

def choose_total_sex_code(sex_var: Dict[str, Any]) -> str:
    vals = sex_var.get("values", []) or []
    txts = sex_var.get("valueTexts", vals) or []
    for code, txt in zip(vals, txts):
        t = fold(txt)
        if t in ["mehed ja naised", "kokku", "total"] or ("mehed" in t and "naised" in t):
            return str(code)
    raise HTTPException(status_code=500, detail={"msg": "Could not detect 'Mehed ja naised' sex code.", "texts_preview": txts[:25]})

def choose_men_women_codes(sex_var: Dict[str, Any]) -> Tuple[str, str]:
    vals = sex_var.get("values", []) or []
    txts = sex_var.get("valueTexts", vals) or []
    men = None
    women = None
    for code, txt in zip(vals, txts):
        t = fold(txt)
        if t == "mehed":
            men = str(code)
        if t == "naised":
            women = str(code)
    if not men or not women:
        raise HTTPException(status_code=500, detail={"msg": "Could not detect 'Mehed'/'Naised' codes.", "texts_preview": txts[:25]})
    return men, women

def resolve_sex_values(sex_var: Dict[str, Any], sex_filter: Optional[str]) -> Tuple[List[str], List[str]]:
    f = fold(sex_filter or "total")
    total_code = choose_total_sex_code(sex_var)
    men_code, women_code = choose_men_women_codes(sex_var)

    if f in {"total", "all", "both", "kokku", "mehed ja naised", "mehedjanaised"}:
        return [total_code], []
    if f in {"men", "mehed", "m", "male"}:
        return [men_code], []
    if f in {"women", "naised", "f", "female"}:
        return [women_code], []

    raise HTTPException(status_code=400, detail={"msg": "Invalid sex_filter. Use: total | men | women", "sex_filter": sex_filter})

def pick_kogu_eesti_code(values: List[str], value_texts: List[str]) -> Optional[str]:
    for code, txt in zip(values, value_texts):
        if fold(clean_value_text(txt)) == "kogu eesti":
            return code
    return None

def pick_indicator_value(var: Dict[str, Any]) -> Optional[str]:
    txt = fold(var.get("text", ""))
    if "naitaja" in txt or "indicator" in txt or "measure" in txt:
        values = var.get("values", []) or []
        texts = var.get("valueTexts", values) or []
        for v, t in zip(values, texts):
            if "rahvaarv" in fold(t) or "inimes" in fold(t) or "population" in fold(t):
                return str(v)
        if values:
            return str(values[0])
    return None

def parse_numeric_age(label: str) -> int:
    digits = re.findall(r"\d+", str(label))
    if not digits:
        raise ValueError(f"Could not parse numeric age from: {label}")
    return int(digits[0])


# ---------- RV0240 exact-age bucketing ----------

def make_buckets_first_to_24(a_from: int, a_to: int, step: int) -> List[Tuple[int, int]]:
    if step == 1:
        return [(a, a) for a in range(a_from, a_to + 1)]

    buckets: List[Tuple[int, int]] = []
    if a_from > a_to:
        return buckets

    if a_from <= 24:
        first_end = min(24, a_to)
        buckets.append((a_from, first_end))
        cur = first_end + 1
        if cur < 25 and a_to >= 25:
            cur = 25
    else:
        cur = a_from

    while cur <= a_to:
        buckets.append((cur, min(cur + step - 1, a_to)))
        cur += step

    return buckets

def build_age_to_bucket_index(buckets: List[Tuple[int, int]]) -> Dict[int, int]:
    m: Dict[int, int] = {}
    for bi, (b0, b1) in enumerate(buckets):
        for age in range(b0, b1 + 1):
            m[age] = bi
    return m


# ================= RV0240 core fetch/sum =================

async def rv0240_fetch(
    year: int,
    wanted_ages: List[str],
    sex_values: List[str],
    residence_values: List[str],
) -> Dict[str, Any]:
    table = "RV0240"
    meta = await px_meta(table)
    vars_ = meta.get("variables", [])

    year_var = pick_var(vars_, ["aasta", "year"])
    age_var = pick_var(vars_, ["vanus", "age"])
    sex_var = pick_var(vars_, ["sugu", "sex"])
    res_var = pick_var(vars_, ["elukoht", "residence", "place of residence"])

    years = year_var.get("values", [])
    if str(year) not in years:
        raise HTTPException(status_code=400, detail={"msg": f"Year {year} not available in RV0240", "available_years": years})

    age_values_set = set(age_var.get("values", []) or [])
    missing = [a for a in wanted_ages if a not in age_values_set]
    if missing:
        raise HTTPException(status_code=400, detail={"msg": "Some requested ages are not available in RV0240.", "missing_ages_preview": missing[:25]})

    query = []
    for v in vars_:
        code = v["code"]
        if code == year_var["code"]:
            query.append({"code": code, "selection": {"filter": "item", "values": [str(year)]}})
        elif code == sex_var["code"]:
            query.append({"code": code, "selection": {"filter": "item", "values": sex_values}})
        elif code == age_var["code"]:
            query.append({"code": code, "selection": {"filter": "item", "values": wanted_ages}})
        elif code == res_var["code"]:
            query.append({"code": code, "selection": {"filter": "item", "values": residence_values}})
        else:
            picked = pick_indicator_value(v)
            vals = v.get("values", []) or []
            if picked:
                query.append({"code": code, "selection": {"filter": "item", "values": [picked]}})
            elif vals:
                query.append({"code": code, "selection": {"filter": "item", "values": [str(vals[0])]}})
            else:
                query.append({"code": code, "selection": {"filter": "all", "values": ["*"]}})

    return await px_query(table, query)

def rv0240_sum_by_dim(js: Dict[str, Any], dim_code: str) -> Tuple[List[str], List[str], List[int]]:
    ids, inv, labels, rows = parse_jsonstat(js)
    if dim_code not in ids:
        raise HTTPException(status_code=500, detail={"msg": "Dimension missing in RV0240 response", "dim": dim_code, "ids": ids})

    d_pos = ids.index(dim_code)
    keys = inv[dim_code]
    label_map = labels[dim_code]

    pops = [0] * len(keys)
    for coords, v in rows:
        pops[coords[d_pos]] += v

    labs = [clean_value_text(label_map.get(k, k)) for k in keys]
    return keys, labs, pops

def rv0240_detect_residence_lists(meta_vars: List[Dict[str, Any]]) -> Tuple[Dict[str, str], str]:
    res_var = pick_var(meta_vars, ["elukoht", "residence", "place of residence"])
    res_code = res_var["code"]
    vals = res_var.get("values", []) or []
    txts = res_var.get("valueTexts", vals) or []
    code_to_text = {}
    for c, t in zip(vals, txts):
        code_to_text[str(c)] = clean_value_text(t)
    return code_to_text, res_code

def is_county_label(label: str) -> bool:
    if "." in label:
        return False
    if ":" in label:
        return False
    if "asustuspiirkond" in fold(label):
        return False
    if fold(label) in ["maakond teadmata", "teadmata"]:
        return False
    return bool(re.match(r"^[A-ZÄÖÜÕ\- ]+ MAAKOND$", label.strip()))

def is_tallinn_district_label(label: str) -> bool:
    t = fold(label)
    return "linnaosa" in t and any(
        t.startswith(fold(x)) for x in [
            "Haabersti", "Kesklinna", "Kristiine", "Lasnamäe",
            "Mustamäe", "Nõmme", "Pirita", "Põhja-Tallinna"
        ]
    )

def is_region5_label(label: str) -> bool:
    return label.strip() in REGION5

def city_match_exact(label: str, city: str) -> bool:
    l = label.strip()
    return l == city or l == f"{city} linn"


# ================= NEW HELPERS for Harju without Tallinn =================

def find_county_code_exact(code_to_text: Dict[str, str], county_name: str) -> Optional[str]:
    target = fold(county_name)
    for c, t in code_to_text.items():
        if fold(clean_value_text(t)) == target:
            return str(c)
    return None

def find_city_code_exact(code_to_text: Dict[str, str], city: str) -> Optional[str]:
    for c, t in code_to_text.items():
        if city_match_exact(clean_value_text(t), city):
            return str(c)
    return None

def find_res_code_contains(code_to_text: Dict[str, str], required_substrings: List[str]) -> Optional[str]:
    """
    Find a residence code whose cleaned label contains ALL required substrings (folded).
    Example:
      required_substrings=["linnaline", "asustuspiirkond"]
    """
    req = [fold(x) for x in required_substrings]
    for c, t in code_to_text.items():
        tt = fold(clean_value_text(t))
        if all(r in tt for r in req):
            return str(c)
    return None


# ================= Base: RV0240 age_group + sex =================

async def quotas_from_rv0240_base_agegroup_and_sex(req: QuotaRequest) -> Dict[str, Any]:
    meta = await px_meta("RV0240")
    vars_ = meta.get("variables", [])

    year_var = pick_var(vars_, ["aasta", "year"])
    age_var = pick_var(vars_, ["vanus", "age"])
    sex_var = pick_var(vars_, ["sugu", "sex"])
    res_var = pick_var(vars_, ["elukoht", "residence", "place of residence"])

    years = year_var.get("values", [])
    if str(req.reference.year) not in years:
        raise HTTPException(status_code=400, detail={"msg": f"Year {req.reference.year} not available in RV0240", "available_years": years})

    a_from, a_to = req.age_band.from_age, req.age_band.to_age
    if a_from > a_to:
        raise HTTPException(status_code=400, detail="age_band.from must be <= age_band.to")

    res_values = res_var.get("values", [])
    res_texts = res_var.get("valueTexts", res_values)
    kogu_eesti = pick_kogu_eesti_code(res_values, res_texts)
    if not kogu_eesti:
        raise HTTPException(status_code=500, detail="Could not find 'Kogu Eesti' in RV0240.")
    residence_filter = [kogu_eesti]

    wanted_ages = [str(a) for a in range(a_from, a_to + 1)]
    results: Dict[str, Any] = {}

    sex_values, _ = resolve_sex_values(sex_var, req.sex_filter)

    js = await rv0240_fetch(
        year=req.reference.year,
        wanted_ages=wanted_ages,
        sex_values=sex_values,
        residence_values=residence_filter,
    )

    ids, inv, labels, rows = parse_jsonstat(js)
    a_pos = ids.index(age_var["code"])
    age_keys = inv[age_var["code"]]
    age_labels_map = labels[age_var["code"]]
    age_idx_to_age = [parse_numeric_age(str(age_labels_map.get(k, k))) for k in age_keys]

    buckets = make_buckets_first_to_24(a_from, a_to, int(req.age_grouping_years))
    age_to_bucket = build_age_to_bucket_index(buckets)

    bucket_pops = [0] * len(buckets)
    for coords, v in rows:
        age_num = age_idx_to_age[coords[a_pos]]
        if a_from <= age_num <= a_to:
            bi = age_to_bucket.get(age_num)
            if bi is not None:
                bucket_pops[bi] += v

    population_total = sum(bucket_pops)

    if "age_group" in req.dimensions:
        age_ids = [f"{b0}-{b1}" for (b0, b1) in buckets]
        age_labs = [f"{b0}-{b1}" for (b0, b1) in buckets]
        age_base, age_cells = compute_cells(age_ids, age_labs, bucket_pops, req.sample_n)
        results["age_group"] = DimensionResult(
            base=age_base,
            cells=age_cells,
            notes=[
                "Bucketing: first bucket ends at age 24 unless chosen bucketing has step 1 year",
            ],
        ).model_dump()

    if "sex" in req.dimensions:
        men_code, women_code = choose_men_women_codes(sex_var)
        js2 = await rv0240_fetch(req.reference.year, wanted_ages, [men_code, women_code], residence_filter)
        ids2, inv2, labels2, rows2 = parse_jsonstat(js2)
        s_pos = ids2.index(sex_var["code"])
        sex_keys = inv2[sex_var["code"]]
        sex_labels_map = labels2[sex_var["code"]]

        pop_by_sex = {k: 0 for k in sex_keys}
        for coords, v in rows2:
            pop_by_sex[sex_keys[coords[s_pos]]] += v

        f = fold(req.sex_filter or "total")
        if f in {"men", "mehed", "m", "male"}:
            out_ids = [men_code]
            out_labs = [clean_value_text(sex_labels_map.get(men_code, "Mehed"))]
            out_pops = [pop_by_sex.get(men_code, 0)]
            notes = []
        elif f in {"women", "naised", "f", "female"}:
            out_ids = [women_code]
            out_labs = [clean_value_text(sex_labels_map.get(women_code, "Naised"))]
            out_pops = [pop_by_sex.get(women_code, 0)]
            notes = []
        else:
            out_ids = [men_code, women_code]
            out_labs = [
                clean_value_text(sex_labels_map.get(men_code, "Mehed")),
                clean_value_text(sex_labels_map.get(women_code, "Naised")),
            ]
            out_pops = [pop_by_sex.get(men_code, 0), pop_by_sex.get(women_code, 0)]
            notes = []

        s_base, s_cells = compute_cells(out_ids, out_labs, out_pops, req.sample_n)
        results["sex"] = DimensionResult(base=s_base, cells=s_cells, notes=notes).model_dump()

    return {
        "population_total": population_total,
        "results": results,
        "meta": {
            "source": "RV0240",
            "year": req.reference.year,
            "age_band": {"from": a_from, "to": a_to},
            "bucket_years": int(req.age_grouping_years),
            "sex_filter": req.sex_filter,
        },
    }


# ================= RV0240 derived dims =================

# county: Tallinn + Harju (ilma Tallinnata) FIRST
async def quotas_rv0240_county(req: QuotaRequest) -> Dict[str, Any]:
    meta = await px_meta("RV0240")
    vars_ = meta.get("variables", [])
    sex_var = pick_var(vars_, ["sugu", "sex"])
    code_to_text, res_code = rv0240_detect_residence_lists(vars_)

    a_from, a_to = req.age_band.from_age, req.age_band.to_age
    wanted_ages = [str(a) for a in range(a_from, a_to + 1)]
    sex_values, _ = resolve_sex_values(sex_var, req.sex_filter)

    county_codes = [
        c for c, t in code_to_text.items()
        if is_county_label(t) and fold(t) not in {"maakond teadmata", "teadmata"}
    ]
    if not county_codes:
        raise HTTPException(status_code=500, detail={"msg": "No county codes detected in RV0240."})

    harju_code = find_county_code_exact(code_to_text, "Harju maakond")
    tallinn_code = find_city_code_exact(code_to_text, "Tallinn")

    residence_values = list(county_codes)
    if tallinn_code and tallinn_code not in residence_values:
        residence_values.append(tallinn_code)

    js = await rv0240_fetch(req.reference.year, wanted_ages, sex_values, residence_values)
    keys, labs, pops = rv0240_sum_by_dim(js, res_code)
    pop_by_code = {k: int(p) for k, p in zip(keys, pops)}

    out_ids: List[str] = []
    out_labs: List[str] = []
    out_pops: List[int] = []

    if harju_code and tallinn_code:
        harju_pop = int(pop_by_code.get(harju_code, 0))
        tallinn_pop = int(pop_by_code.get(tallinn_code, 0))
        harju_wo = max(0, harju_pop - tallinn_pop)

        out_ids.append(tallinn_code)
        out_labs.append("Tallinn")
        out_pops.append(tallinn_pop)

        out_ids.append(harju_code)
        out_labs.append("Harju Maakond (ilma Tallinnata)")
        out_pops.append(harju_wo)
    else:
        if tallinn_code:
            out_ids.append(tallinn_code)
            out_labs.append("Tallinn")
            out_pops.append(int(pop_by_code.get(tallinn_code, 0)))
        if harju_code and harju_code in county_codes:
            lab = clean_value_text(code_to_text.get(harju_code, harju_code))
            out_ids.append(harju_code)
            out_labs.append(lab.title())
            out_pops.append(int(pop_by_code.get(harju_code, 0)))

    for k in county_codes:
        if harju_code and k == harju_code:
            continue
        lab = clean_value_text(code_to_text.get(k, k))
        if not is_county_label(lab):
            continue
        out_ids.append(k)
        out_labs.append(lab.title())
        out_pops.append(int(pop_by_code.get(k, 0)))

    notes: List[str] = []
    base, cells = compute_cells(out_ids, out_labs, out_pops, req.sample_n)
    return {
        "population_total": base,
        "results": {"county": DimensionResult(base=base, cells=cells, notes=notes).model_dump()},
        "meta": {"source": "RV0240", "sex_filter": req.sex_filter},
    }

async def quotas_rv0240_region5(req: QuotaRequest) -> Dict[str, Any]:
    meta = await px_meta("RV0240")
    vars_ = meta.get("variables", [])
    sex_var = pick_var(vars_, ["sugu", "sex"])
    code_to_text, res_code = rv0240_detect_residence_lists(vars_)

    a_from, a_to = req.age_band.from_age, req.age_band.to_age
    wanted_ages = [str(a) for a in range(a_from, a_to + 1)]
    sex_values, _ = resolve_sex_values(sex_var, req.sex_filter)

    region_codes = [c for c, t in code_to_text.items() if is_region5_label(t)]
    if not region_codes:
        raise HTTPException(status_code=500, detail={"msg": "No region codes detected in RV0240.", "expected": sorted(list(REGION5))})

    js = await rv0240_fetch(req.reference.year, wanted_ages, sex_values, region_codes)
    keys, labs, pops = rv0240_sum_by_dim(js, res_code)

    out_ids, out_labs, out_pops = [], [], []
    for k, lab, pop in zip(keys, labs, pops):
        if is_region5_label(lab):
            out_ids.append(k)
            out_labs.append(lab)
            out_pops.append(int(pop))

    notes = []
    base, cells = compute_cells(out_ids, out_labs, out_pops, req.sample_n)
    return {"population_total": base, "results": {"region": DimensionResult(base=base, cells=cells, notes=notes).model_dump()}, "meta": {"source": "RV0240", "sex_filter": req.sex_filter}}

async def quotas_rv0240_tallinn_districts(req: QuotaRequest) -> Dict[str, Any]:
    meta = await px_meta("RV0240")
    vars_ = meta.get("variables", [])
    sex_var = pick_var(vars_, ["sugu", "sex"])
    code_to_text, res_code = rv0240_detect_residence_lists(vars_)

    a_from, a_to = req.age_band.from_age, req.age_band.to_age
    wanted_ages = [str(a) for a in range(a_from, a_to + 1)]
    sex_values, _ = resolve_sex_values(sex_var, req.sex_filter)

    district_codes = [c for c, t in code_to_text.items() if is_tallinn_district_label(t)]
    if not district_codes:
        raise HTTPException(status_code=500, detail={"msg": "No Tallinn district codes detected in RV0240."})

    js = await rv0240_fetch(req.reference.year, wanted_ages, sex_values, district_codes)
    keys, labs, pops = rv0240_sum_by_dim(js, res_code)

    out_ids, out_labs, out_pops = [], [], []
    for k, lab, pop in zip(keys, labs, pops):
        if is_tallinn_district_label(lab):
            out_ids.append(k)
            out_labs.append(lab)
            out_pops.append(int(pop))

    notes = []
    base, cells = compute_cells(out_ids, out_labs, out_pops, req.sample_n)
    return {"population_total": base, "results": {"tallinn_districts": DimensionResult(base=base, cells=cells, notes=notes).model_dump()}, "meta": {"source": "RV0240", "sex_filter": req.sex_filter}}

# UPDATED per your rules:
# - Pealinn = Tallinn
# - Suurlinnad = Tartu, Pärnu, Narva, Kohtla-Järve
# - Muu linn = (linnaline asustuspiirkond + väikelinnaline asustuspiirkond) - (Pealinn + Suurlinnad)
# - Maa = maaline asustuspiirkond 
async def quotas_rv0240_settlement_type4(req: QuotaRequest) -> Dict[str, Any]:
    meta = await px_meta("RV0240")
    vars_ = meta.get("variables", [])
    sex_var = pick_var(vars_, ["sugu", "sex"])
    res_var = pick_var(vars_, ["elukoht", "residence", "place of residence"])
    code_to_text, res_code = rv0240_detect_residence_lists(vars_)

    a_from, a_to = req.age_band.from_age, req.age_band.to_age
    wanted_ages = [str(a) for a in range(a_from, a_to + 1)]
    sex_values, _ = resolve_sex_values(sex_var, req.sex_filter)

    res_values = res_var.get("values", []) or []
    res_texts = res_var.get("valueTexts", res_values) or []
    kogu_eesti_code = pick_kogu_eesti_code(res_values, res_texts)
    if not kogu_eesti_code:
        raise HTTPException(status_code=500, detail="Could not find 'Kogu Eesti' in RV0240.")

    # Exact city codes
    tallinn_code = find_city_code_exact(code_to_text, "Tallinn")
    if not tallinn_code:
        raise HTTPException(status_code=500, detail={"msg": "Could not find Tallinn code in RV0240 residence list."})

    big_city_codes: List[str] = []
    missing_big: List[str] = []
    for city in sorted(BIG_CITIES):
        cc = find_city_code_exact(code_to_text, city)
        if cc:
            big_city_codes.append(cc)
        else:
            missing_big.append(city)

    # Settlement-type category codes
    linnaline_code = (
        find_res_code_contains(code_to_text, ["linnaline", "asustuspiirkond"])
        or find_res_code_contains(code_to_text, ["linnaline"])
    )
    vaikelinnaline_code = (
        find_res_code_contains(code_to_text, ["vaikelinnaline", "asustuspiirkond"])
        or find_res_code_contains(code_to_text, ["väikelinnaline", "asustuspiirkond"])
        or find_res_code_contains(code_to_text, ["vaikelinnaline"])
        or find_res_code_contains(code_to_text, ["väikelinnaline"])
    )
    maaline_code = (
        find_res_code_contains(code_to_text, ["maaline", "asustuspiirkond"])
        or find_res_code_contains(code_to_text, ["maaline"])
    )

    if not linnaline_code or not vaikelinnaline_code or not maaline_code:
        raise HTTPException(
            status_code=500,
            detail={
                "msg": "Could not detect settlement type codes in RV0240 residence list.",
                "found": {
                    "linnaline": linnaline_code,
                    "vaikelinnaline": vaikelinnaline_code,
                    "maaline": maaline_code,
                },
                "hint": "Check residence valueTexts for 'linnaline/vaikelinnaline/maaline asustuspiirkond'.",
            },
        )

    needed_codes = [kogu_eesti_code, tallinn_code] + big_city_codes + [linnaline_code, vaikelinnaline_code, maaline_code]
    needed_codes = list(dict.fromkeys(needed_codes))

    js = await rv0240_fetch(req.reference.year, wanted_ages, sex_values, needed_codes)
    keys, labs, pops = rv0240_sum_by_dim(js, res_code)
    pop_by_code = {k: int(p) for k, p in zip(keys, pops)}

    capital_pop = pop_by_code.get(tallinn_code, 0)
    big_pop = sum(pop_by_code.get(c, 0) for c in big_city_codes)

    linnaline_pop = pop_by_code.get(linnaline_code, 0)
    vaikelinnaline_pop = pop_by_code.get(vaikelinnaline_code, 0)
    maa_pop = pop_by_code.get(maaline_code, 0)

    muu_linn_pop = max(0, (linnaline_pop + vaikelinnaline_pop) - (capital_pop + big_pop))

    out_ids = ["pealinn", "suurlinn", "muulinn", "maa"]
    out_labs = [
        "Pealinn (Tallinn)",
        "Suurlinnad (Tartu, Pärnu, Narva, Kohtla-Järve)",
        "Muu linn (linnaline + väikelinnaline, ilma pealinna ja suurlinnadeta)",
        "Maa (maaline asustuspiirkond)",
    ]
    out_pops = [capital_pop, big_pop, muu_linn_pop, maa_pop]

    notes = [
        f"Suurlinnad: {', '.join(sorted(list(BIG_CITIES)))}" + (f" (missing: {', '.join(missing_big)})" if missing_big else ""),
        "Muu linn = (linnaline asustuspiirkond + väikelinnaline asustuspiirkond) - (pealinn + suurlinnad).",
        "Maa = maaline asustuspiirkond.",
    ]
    base, cells = compute_cells(out_ids, out_labs, out_pops, req.sample_n)
    return {
        "population_total": base,
        "results": {"settlement_type": DimensionResult(base=base, cells=cells, notes=notes).model_dump()},
        "meta": {"source": "RV0240", "sex_filter": req.sex_filter},
    }


# ================= AGE-GROUP TABLE HELPERS =================

def parse_age_group_range(label: str) -> Optional[Tuple[int, int]]:
    s = str(label).strip()
    s = s.replace("–", "-").replace("—", "-")
    s = s.replace(" aastat", "").replace(" a", "").strip()
    if "+" in s:
        left = s.split("+")[0]
        nums = re.findall(r"\d+", left)
        if not nums:
            return None
        return (int(nums[0]), 200)
    if "-" in s:
        parts = s.split("-")
        if len(parts) >= 2:
            a_nums = re.findall(r"\d+", parts[0])
            b_nums = re.findall(r"\d+", parts[1])
            if not a_nums or not b_nums:
                return None
            return (int(a_nums[0]), int(b_nums[0]))
    nums = re.findall(r"\d+", s)
    if nums:
        a = int(nums[0])
        return (a, a)
    return None

def select_agegroups_overlap_with_notes(age_values: List[str], age_texts: List[str], req_from: int, req_to: int) -> Tuple[List[str], List[str]]:
    parsed = [(str(c), str(t), parse_age_group_range(t)) for c, t in zip(age_values, age_texts)]
    chosen = []
    for code, txt, r in parsed:
        if r is None:
            continue
        a, b = r
        if not (b < req_from or a > req_to):
            chosen.append((code, txt, a, b))
    if not chosen:
        avail = [t for _, t, r in parsed if r is not None]
        raise HTTPException(status_code=400, detail={"msg": "No age groups overlap requested range.", "requested": {"from": req_from, "to": req_to}, "available_preview": avail[:60]})

    used_from = min(a for _, _, a, _ in chosen)
    used_to = max(b for _, _, _, b in chosen)
    used_groups = [t for _, t, _, _ in chosen]

    notes = [
        "⚠️ This quota uses AGE GROUPS (not single-year ages).",
        f"Requested ages: {req_from}–{req_to}.",
        f"Actually used (FULL overlapping age groups): {used_from}–{used_to}.",
        "Used age groups: " + ", ".join(used_groups),
    ]
    if used_from < req_from:
        notes.append(f"Includes younger ages too: {used_from}–{req_from-1}.")
    if used_to > req_to:
        notes.append(f"Includes older ages too: {req_to+1}–{used_to}.")
    chosen_codes = {c for c, _, _, _ in chosen}
    codes_in_order = [c for c, _, _ in parsed if c in chosen_codes]
    return codes_in_order, notes

def find_total_value_code(var: Dict[str, Any]) -> Optional[str]:
    vals = var.get("values", []) or []
    txts = var.get("valueTexts", vals) or []
    for code, txt in zip(vals, txts):
        t = fold(clean_value_text(txt))
        if t in ["kogu eesti", "eesti"]:
            return str(code)
    for code, txt in zip(vals, txts):
        t = fold(txt)
        if "kogu eesti" in t or t == "eesti":
            return str(code)
    return None

def make_geo_selection_total_or_eliminate(var: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    notes: List[str] = []
    code = find_total_value_code(var)
    if code:
        return ({"filter": "item", "values": [code]}, notes)
    if var.get("elimination", False):
        notes.append("Geography: maakond eliminated (PxWeb elimination) to get Estonia total.")
        return (None, notes)
    vals = var.get("values", []) or []
    txts = var.get("valueTexts", vals) or []
    if vals:
        notes.append("⚠️ Geography: could not find total and elimination not available; falling back to first maakond value (may be wrong).")
        notes.append(f"Fallback maakond: {txts[0] if txts else vals[0]}")
        return ({"filter": "item", "values": [str(vals[0])]}, notes)
    notes.append("⚠️ Geography: no values found.")
    return (None, notes)

def is_totalish(label: str) -> bool:
    t = fold(label)
    return t == "kokku" or "kokku" in t or t == "total" or "total" in t


# ================= RV022U nationality =================

async def quotas_nationality(req: QuotaRequest) -> Dict[str, Any]:
    table = "RV022U"
    meta = await px_meta(table)
    vars_ = meta.get("variables", [])

    year_var = pick_var(vars_, ["aasta", "year"])
    sex_var = pick_var(vars_, ["sugu", "sex"])
    ageg_var = pick_var(vars_, ["vanuserühm", "vanuseruhm", "vanusrühm", "vanusr", "age group", "agegroup"])
    nat_var = pick_var(vars_, ["rahvus", "nationality"])
    geo_var = pick_var(vars_, ["maakond", "county"])

    years = year_var.get("values", [])
    if str(req.reference.year) not in years:
        raise HTTPException(status_code=400, detail={"msg": f"Year {req.reference.year} not available in {table}", "available_years": years})

    sex_values, _ = resolve_sex_values(sex_var, req.sex_filter)

    age_values = ageg_var.get("values", []) or []
    age_texts = ageg_var.get("valueTexts", age_values) or []
    age_codes, age_notes = select_agegroups_overlap_with_notes(age_values, age_texts, req.age_band.from_age, req.age_band.to_age)

    geo_sel, geo_notes = make_geo_selection_total_or_eliminate(geo_var)

    query = []
    for v in vars_:
        code = v["code"]
        if code == year_var["code"]:
            query.append({"code": code, "selection": {"filter": "item", "values": [str(req.reference.year)]}})
        elif code == sex_var["code"]:
            query.append({"code": code, "selection": {"filter": "item", "values": sex_values}})
        elif code == ageg_var["code"]:
            query.append({"code": code, "selection": {"filter": "item", "values": age_codes}})
        elif code == geo_var["code"]:
            if geo_sel is None:
                continue
            query.append({"code": code, "selection": geo_sel})
        else:
            query.append({"code": code, "selection": {"filter": "all", "values": ["*"]}})

    js = await px_query(table, query)
    ids, inv, labels, rows = parse_jsonstat(js)

    n_pos = ids.index(nat_var["code"])
    n_keys = inv[nat_var["code"]]
    n_label_map = labels[nat_var["code"]]

    pop_by_key = {k: 0 for k in n_keys}
    for coords, v in rows:
        pop_by_key[n_keys[coords[n_pos]]] += v

    eestlased = venelased = ukrainlased = muud = 0
    teadmata_skipped = 0

    for k in n_keys:
        label = str(n_label_map.get(k, k))
        p = int(pop_by_key.get(k, 0))
        t = fold(label)

        if is_totalish(label) or "rahvus kokku" in t or "rahvused kokku" in t:
            continue
        if "teadmata" in t:
            teadmata_skipped += p
            continue

        if "eestl" in t:
            eestlased += p
        elif "venel" in t:
            venelased += p
        elif "ukrain" in t:
            ukrainlased += p
        else:
            muud += p

    out_ids = ["eestlased", "venelased", "ukrainlased", "muud"]
    out_labels = ["Eestlased", "Venelased", "Ukrainlased", "Muud rahvused"]
    out_pops = [eestlased, venelased, ukrainlased, muud]

    notes = []
    notes.extend(age_notes)
    notes.extend(geo_notes)
    notes.append("Unknown Nationality is EXCLUDED.")
    notes.append(f"Skipped teadmata pop: {teadmata_skipped}")

    base, cells = compute_cells(out_ids, out_labels, out_pops, req.sample_n)
    return {"population_total": base, "results": {"nationality": DimensionResult(base=base, cells=cells, notes=notes).model_dump()}, "meta": {"source": table, "sex_filter": req.sex_filter}}


# ================= RV0231U education =================

async def quotas_education(req: QuotaRequest) -> Dict[str, Any]:
    table = "RV0231U"
    meta = await px_meta(table)
    vars_ = meta.get("variables", [])

    year_var = pick_var(vars_, ["aasta", "year"])
    sex_var = pick_var(vars_, ["sugu", "sex"])
    ageg_var = pick_var(vars_, ["vanuserühm", "vanuseruhm", "vanusrühm", "vanusr", "age group", "agegroup"])
    edu_var = pick_var(vars_, ["haridus", "education"])
    geo_var = pick_var(vars_, ["maakond", "county"])

    years = year_var.get("values", [])
    if str(req.reference.year) not in years:
        raise HTTPException(status_code=400, detail={"msg": f"Year {req.reference.year} not available in {table}", "available_years": years})

    sex_values, _ = resolve_sex_values(sex_var, req.sex_filter)

    age_values = ageg_var.get("values", []) or []
    age_texts = ageg_var.get("valueTexts", age_values) or []
    age_codes, age_notes = select_agegroups_overlap_with_notes(age_values, age_texts, req.age_band.from_age, req.age_band.to_age)

    geo_sel, geo_notes = make_geo_selection_total_or_eliminate(geo_var)

    edu_values = edu_var.get("values", []) or []
    wanted_edu = [str(v) for v in edu_values if str(v) in ALLOWED_EDUCATION_IDS]
    if not wanted_edu:
        raise HTTPException(status_code=500, detail={"msg": "None of education IDs 2,7,11 found in table.", "table": table})

    query = []
    for v in vars_:
        code = v["code"]
        if code == year_var["code"]:
            query.append({"code": code, "selection": {"filter": "item", "values": [str(req.reference.year)]}})
        elif code == sex_var["code"]:
            query.append({"code": code, "selection": {"filter": "item", "values": sex_values}})
        elif code == ageg_var["code"]:
            query.append({"code": code, "selection": {"filter": "item", "values": age_codes}})
        elif code == geo_var["code"]:
            if geo_sel is None:
                continue
            query.append({"code": code, "selection": geo_sel})
        elif code == edu_var["code"]:
            query.append({"code": code, "selection": {"filter": "item", "values": wanted_edu}})
        else:
            query.append({"code": code, "selection": {"filter": "all", "values": ["*"]}})

    js = await px_query(table, query)
    ids, inv, labels, rows = parse_jsonstat(js)

    e_pos = ids.index(edu_var["code"])
    e_keys = inv[edu_var["code"]]
    e_label_map = labels[edu_var["code"]]

    pops = [0] * len(e_keys)
    for coords, v in rows:
        pops[coords[e_pos]] += v

    labs = [str(e_label_map.get(k, k)) for k in e_keys]

    f_ids, f_labs, f_pops = [], [], []
    for k, lab, pop in zip(e_keys, labs, pops):
        if "teadmata" in fold(lab):
            continue
        f_ids.append(k)
        f_labs.append(clean_value_text(lab))
        f_pops.append(int(pop))

    notes = []
    notes.extend(age_notes)
    notes.extend(geo_notes)
    notes.append("Education Unknown is excluded.")

    base, cells = compute_cells(f_ids, f_labs, f_pops, req.sample_n)
    return {"population_total": base, "results": {"education": DimensionResult(base=base, cells=cells, notes=notes).model_dump()}, "meta": {"source": table, "sex_filter": req.sex_filter}}


# ================= RV069U birth/citizenship by country (AGGREGATED) =================

def pick_birthcit_type_codes(var: Dict[str, Any]) -> Tuple[str, str]:
    vals = var.get("values", []) or []
    txts = var.get("valueTexts", vals) or []
    birth = None
    citizen = None
    for c, t in zip(vals, txts):
        tt = fold(clean_value_text(t))
        if "synniriik" in tt or "sunniriik" in tt or "birth" in tt:
            birth = str(c)
        if "kodakondsus" in tt or "citizen" in tt:
            citizen = str(c)
    if not birth or not citizen:
        raise HTTPException(status_code=500, detail={"msg": "Could not detect codes for Sünniriik / Kodakondsus in RV069U.", "texts_preview": txts[:40]})
    return birth, citizen

def is_unknown_country(code: str, label: str) -> bool:
    c = str(code).strip().upper()
    t = fold(label)
    return c in {"XX", "UNK"} or "maaramata" in t or "maaramaata" in t or "unknown" in t or "teadmata" in t

def is_eu_excl_ee_bucket(code: str, label: str) -> bool:
    t = fold(label)
    return "el-i riik" in t or ("euroopa liit" in t and "v.a eesti" in t) or ("eu" in t and "eesti" in t and "va" in t)

def is_non_eu_bucket(code: str, label: str) -> bool:
    t = fold(label)
    return "valisriik" in t and "el" in t and ("v.a" in t or "va" in t)

async def quotas_rv069u_country_aggregated(req: QuotaRequest, mode: str) -> Dict[str, Any]:
    table = "RV069U"
    meta = await px_meta(table)
    vars_ = meta.get("variables", [])

    year_var = pick_var(vars_, ["aasta", "year"])
    sex_var = pick_var(vars_, ["sugu", "sex"])
    ageg_var = pick_var(vars_, ["vanusrühm", "vanusr", "age group", "agegroup"])
    geo_var = pick_var(vars_, ["maakond", "county"])

    type_var = pick_var_exact(vars_, "Sünniriik/Kodakondsus")
    country_var = pick_var_exact(vars_, "Riik")

    years = year_var.get("values", [])
    if str(req.reference.year) not in years:
        raise HTTPException(status_code=400, detail={"msg": f"Year {req.reference.year} not available in {table}", "available_years": years})

    sex_values, _ = resolve_sex_values(sex_var, req.sex_filter)

    age_values = ageg_var.get("values", []) or []
    age_texts = ageg_var.get("valueTexts", age_values) or []
    age_codes, age_notes = select_agegroups_overlap_with_notes(age_values, age_texts, req.age_band.from_age, req.age_band.to_age)

    geo_sel, geo_notes = make_geo_selection_total_or_eliminate(geo_var)

    birth_code, citizen_code = pick_birthcit_type_codes(type_var)
    chosen_type_code = birth_code if mode == "birth" else citizen_code

    query = []
    for v in vars_:
        code = v["code"]
        if code == year_var["code"]:
            query.append({"code": code, "selection": {"filter": "item", "values": [str(req.reference.year)]}})
        elif code == sex_var["code"]:
            query.append({"code": code, "selection": {"filter": "item", "values": sex_values}})
        elif code == ageg_var["code"]:
            query.append({"code": code, "selection": {"filter": "item", "values": age_codes}})
        elif code == geo_var["code"]:
            if geo_sel is None:
                continue
            query.append({"code": code, "selection": geo_sel})
        elif code == type_var["code"]:
            query.append({"code": code, "selection": {"filter": "item", "values": [chosen_type_code]}})
        elif code == country_var["code"]:
            query.append({"code": code, "selection": {"filter": "all", "values": ["*"]}})
        else:
            query.append({"code": code, "selection": {"filter": "all", "values": ["*"]}})

    js = await px_query(table, query)
    ids, inv, labels, rows = parse_jsonstat(js)

    c_pos = ids.index(country_var["code"])
    c_keys = inv[country_var["code"]]
    c_label_map = labels[country_var["code"]]

    pop_by_code: Dict[str, int] = {k: 0 for k in c_keys}
    for coords, v in rows:
        pop_by_code[c_keys[coords[c_pos]]] += v

    ee = 0
    eu_excl_ee = 0
    ru = 0
    ua = 0
    noneu_bucket = 0
    unknown = 0

    for k in c_keys:
        lab = clean_value_text(c_label_map.get(k, k))
        p = int(pop_by_code.get(k, 0))
        code = str(k).strip().upper()

        if is_totalish(lab):
            continue
        if is_unknown_country(code, lab):
            unknown += p
            continue
        if code == "EE":
            ee += p
            continue
        if code == "RU":
            ru += p
            continue
        if code == "UA":
            ua += p
            continue
        if is_eu_excl_ee_bucket(code, lab):
            eu_excl_ee += p
            continue
        if is_non_eu_bucket(code, lab):
            noneu_bucket += p
            continue

    non_eu_excl = max(0, noneu_bucket - ru - ua)
    include_unknown = (mode == "citizenship")

    out_ids = ["EE", "EU_EXCL_EE", "RU", "UA", "NON_EU_EXCL_EU"]
    out_labels = ["Eesti", "EL-i riik (v.a Eesti)", "Venemaa", "Ukraina", "Välisriik (v.a EL-i riigid) (ilma RU/UA)"]
    out_pops = [ee, eu_excl_ee, ru, ua, non_eu_excl]

    if include_unknown:
        out_ids.append("XX")
        out_labels.append("Määramata")
        out_pops.append(unknown)

    notes: List[str] = []
    notes.extend(age_notes)
    notes.extend(geo_notes)

    base, cells = compute_cells(out_ids, out_labels, out_pops, req.sample_n)
    key = "birth_country" if mode == "birth" else "citizenship_country"
    return {"population_total": base, "results": {key: DimensionResult(base=base, cells=cells, notes=notes).model_dump()}, "meta": {"source": table, "mode": key, "sex_filter": req.sex_filter}}


# ================= APP =================

app = FastAPI(title="Norstat Quota API (RV0240 exact-age; RV022U+RV0231U agegroups; RV069U aggregated buckets)")

@app.get("/")
async def root():
    return {"ok": True, "message": "Quota API is running", "docs": "/docs", "health": "/health"}

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "https://quota-frontend-iota.vercel.app",
    ],
    allow_origin_regex=r"^https://.*\.vercel\.app$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(Exception)
async def handler(req, exc):
    if isinstance(exc, HTTPException):
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
    return JSONResponse(status_code=500, content={"error": exc.__class__.__name__, "detail": str(exc)})

@app.get("/health")
async def health():
    return {"ok": True, "pxweb_base": PXWEB_BASE}

@app.post("/v1/quotas/calculate", response_model=QuotaResponse)
async def calculate(req: QuotaRequest, x_api_key: Optional[str] = Header(default=None)):
    require_key(x_api_key)

    normalized = []
    for d in req.dimensions:
        if d == "region5":
            normalized.append("region")
        elif d == "settlement_type4":
            normalized.append("settlement_type")
        else:
            normalized.append(d)
    req.dimensions = normalized

    base_pack = await quotas_from_rv0240_base_agegroup_and_sex(req)
    population_total = base_pack["population_total"]

    results: Dict[str, Any] = dict(base_pack["results"])
    meta: Dict[str, Any] = {"base": base_pack["meta"], "sources": [], "errors": {}}

    supported = {
        "sex", "age_group",
        "county", "region", "tallinn_districts", "settlement_type",
        "nationality", "education",
        "birth_country", "citizenship_country",
    }

    if "county" in req.dimensions:
        try:
            pack = await quotas_rv0240_county(req)
            results.update(pack["results"])
            meta["sources"].append(pack["meta"])
        except HTTPException as e:
            meta["errors"]["county"] = e.detail

    if "region" in req.dimensions:
        try:
            pack = await quotas_rv0240_region5(req)
            results.update(pack["results"])
            meta["sources"].append(pack["meta"])
        except HTTPException as e:
            meta["errors"]["region"] = e.detail

    if "tallinn_districts" in req.dimensions:
        try:
            pack = await quotas_rv0240_tallinn_districts(req)
            results.update(pack["results"])
            meta["sources"].append(pack["meta"])
        except HTTPException as e:
            meta["errors"]["tallinn_districts"] = e.detail

    if "settlement_type" in req.dimensions:
        try:
            pack = await quotas_rv0240_settlement_type4(req)
            results.update(pack["results"])
            meta["sources"].append(pack["meta"])
        except HTTPException as e:
            meta["errors"]["settlement_type"] = e.detail

    if "nationality" in req.dimensions:
        try:
            pack = await quotas_nationality(req)
            results.update(pack["results"])
            meta["sources"].append(pack["meta"])
        except HTTPException as e:
            meta["errors"]["nationality"] = e.detail

    if "education" in req.dimensions:
        try:
            pack = await quotas_education(req)
            results.update(pack["results"])
            meta["sources"].append(pack["meta"])
        except HTTPException as e:
            meta["errors"]["education"] = e.detail

    if "birth_country" in req.dimensions:
        try:
            pack = await quotas_rv069u_country_aggregated(req, mode="birth")
            results.update(pack["results"])
            meta["sources"].append(pack["meta"])
        except HTTPException as e:
            meta["errors"]["birth_country"] = e.detail

    if "citizenship_country" in req.dimensions:
        try:
            pack = await quotas_rv069u_country_aggregated(req, mode="citizenship")
            results.update(pack["results"])
            meta["sources"].append(pack["meta"])
        except HTTPException as e:
            meta["errors"]["citizenship_country"] = e.detail

    for d in req.dimensions:
        if d not in supported and d not in meta["errors"]:
            meta["errors"][d] = {"msg": "Unsupported dimension (not implemented in this version).", "supported": sorted(list(supported))}

    # Deduplicate sources (keep order)
    seen = set()
    uniq = []
    for s in meta["sources"]:
        key = (s.get("source"), s.get("mode"), s.get("sex_filter"))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(s)
    meta["sources"] = uniq

    return {"population_total": population_total, "sample_n": req.sample_n, "results": results, "meta": meta}
