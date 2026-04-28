import json
import math
import re
import sys
import unicodedata
from urllib import error, request


API_BASE = "https://quota-api.vercel.app"
PXWEB_BASE = "https://andmed.stat.ee/api/v1/et/stat"


def http_get_json(url: str):
    with request.urlopen(url, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def http_post_json(url: str, payload):
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(url, data=body, headers={"Content-Type": "application/json"})
    with request.urlopen(req, timeout=120) as response:
        return json.loads(response.read().decode("utf-8"))


def fold(text: str) -> str:
    text = str(text).strip().lower()
    text = re.sub(r"\s+", " ", text)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", text).strip()


def clean_value_text(text: str) -> str:
    return str(text).lstrip(".").strip()


def is_totalish(label: str) -> bool:
    text = fold(label)
    return text == "kokku" or "kokku" in text or text == "total" or "total" in text


def parse_jsonstat(js):
    ds = js.get("dataset", js)
    ids = ds["id"]
    dim = ds["dimension"]
    size = ds["size"]
    vals = ds["value"]

    inv = {}
    labels = {}
    for dim_name in ids:
        cat = dim[dim_name]["category"]
        idx = cat["index"]
        inv_list = [None] * len(idx)
        for key, pos in idx.items():
            inv_list[pos] = key
        inv[dim_name] = inv_list
        labels[dim_name] = cat.get("label", {})

    mult = []
    prod = 1
    for s in reversed(size):
        mult.insert(0, prod)
        prod *= s

    rows = []
    for i, value in enumerate(vals):
        if value is None:
            continue
        coords = [(i // mult[j]) % size[j] for j in range(len(size))]
        rows.append((coords, int(value)))
    return ids, inv, labels, rows


def px_meta(table: str):
    return http_get_json(f"{PXWEB_BASE}/{table}")


def px_query(table: str, query):
    payload = {"query": query, "response": {"format": "json-stat2"}}
    return http_post_json(f"{PXWEB_BASE}/{table}", payload)


def get_var(variables, code_or_text):
    target = fold(code_or_text)
    for var in variables:
        if fold(var.get("code", "")) == target or fold(var.get("text", "")) == target:
            return var
    raise KeyError(code_or_text)


def choose_men_women_codes(sex_var):
    men = None
    women = None
    for code, text in zip(sex_var["values"], sex_var["valueTexts"]):
        ft = fold(text)
        if ft == "mehed":
            men = str(code)
        elif ft == "naised":
            women = str(code)
    if not men or not women:
        raise RuntimeError("Missing men/women codes")
    return men, women


def resolve_query_sex_values(sex_var, sex_filter):
    men, women = choose_men_women_codes(sex_var)
    sf = fold(sex_filter or "total")
    if sf in {"men", "mehed", "m", "male"}:
        return [men], [men]
    if sf in {"women", "naised", "f", "female"}:
        return [women], [women]
    return [men, women], [men, women]


def parse_age_group_range(label: str):
    s = clean_value_text(label)
    sf = fold(s)
    if sf in {"vanuseruhmad kokku", "vanuserühmad kokku"}:
        return None
    if "ja vanemad" in sf:
        nums = re.findall(r"\d+", sf)
        if nums:
            return int(nums[0]), 200
    parts = re.split(r"[-–]", s)
    if len(parts) >= 2:
        a_nums = re.findall(r"\d+", parts[0])
        b_nums = re.findall(r"\d+", parts[1])
        if a_nums and b_nums:
            return int(a_nums[0]), int(b_nums[0])
    nums = re.findall(r"\d+", s)
    if nums:
        age = int(nums[0])
        return age, age
    return None


def select_agegroup_codes(age_var, spans):
    chosen = []
    for code, text in zip(age_var["values"], age_var["valueTexts"]):
        age_range = parse_age_group_range(text)
        if age_range is None:
            continue
        start, end = age_range
        if any(not (end < req_start or start > req_end) for req_start, req_end in spans):
            chosen.append(str(code))
    if not chosen:
        raise RuntimeError("No overlapping age groups found")
    return chosen


def city_aliases(city: str):
    if city == "Tallinn":
        return {"tallinn", "tallinna linn", "tallinn linn"}
    if city == "Tartu":
        return {"tartu", "tartu linn"}
    return {fold(city), fold(f"{city} linn")}


def is_county_label(label: str) -> bool:
    clean = clean_value_text(label)
    text = fold(clean)
    if "." in clean or ":" in clean:
        return False
    if "asustuspiirkond" in text:
        return False
    if text in {"maakond teadmata", "elukoht teadmata", "teadmata"}:
        return False
    return text.endswith(" maakond")


def is_tallinn_district_label(label: str) -> bool:
    text = fold(clean_value_text(label))
    starts = [
        "haabersti",
        "kesklinna",
        "kristiine",
        "lasnamae",
        "mustamae",
        "nomme",
        "pirita",
        "pohja-tallinna",
    ]
    return "linnaosa" in text and any(text.startswith(prefix) for prefix in starts)


def county_output_label(label: str) -> str:
    clean = clean_value_text(label)
    text = fold(clean)
    if text in city_aliases("Tallinn"):
        return "Tallinna linn"
    if text in city_aliases("Tartu"):
        return "Tartu linn"
    return clean.title()


def map_nationality_group(label: str):
    text = fold(label)
    if is_totalish(label) or "rahvus kokku" in text or "rahvused kokku" in text:
        return None
    if "teadmata" in text:
        return None
    if "eestl" in text:
        return "estonian"
    if "venel" in text:
        return "russian"
    if "ukrain" in text:
        return "ukrainian"
    return "other"


def choose_geo_values(var, county_filter=None, for_county=False, for_districts=False):
    values = var.get("values", [])
    texts = var.get("valueTexts", values)

    if for_districts:
        return [str(code) for code, text in zip(values, texts) if is_tallinn_district_label(text)]

    if county_filter:
        target = fold(county_filter)
        for code, text in zip(values, texts):
            clean = clean_value_text(text)
            if target in city_aliases("Tallinn") and fold(clean) in city_aliases("Tallinn"):
                return [str(code)]
            if target in city_aliases("Tartu") and fold(clean) in city_aliases("Tartu"):
                return [str(code)]
            if fold(clean) == target:
                return [str(code)]
        raise RuntimeError(f"Unknown county filter: {county_filter}")

    if for_county:
        chosen = []
        seen = set()
        for code, text in zip(values, texts):
            clean = clean_value_text(text)
            if not (is_county_label(clean) or fold(clean) in city_aliases("Tallinn") or fold(clean) in city_aliases("Tartu")):
                continue
            out_label = county_output_label(clean)
            if out_label in seen:
                continue
            seen.add(out_label)
            chosen.append(str(code))
        return chosen

    total = None
    for code, text in zip(values, texts):
        if fold(clean_value_text(text)) in {"kogu eesti", "eesti"}:
            total = str(code)
            break
    if total:
        return [total]
    return None


def run_api(payload):
    return http_post_json(f"{API_BASE}/v1/quotas/calculate", payload)


def run_api_expect_error(payload):
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(f"{API_BASE}/v1/quotas/calculate", data=body, headers={"Content-Type": "application/json"})
    try:
        with request.urlopen(req, timeout=120) as response:
            return False, json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        payload = exc.read().decode("utf-8")
        return True, payload


def assert_response_invariants(name, response, sample_n):
    failures = []
    for dim, result in response["results"].items():
        base = result["base"]
        cells = result["cells"]
        pop_sum = sum(cell["pop"] for cell in cells)
        quota_sum = sum(cell["quota"] for cell in cells)
        share_sum = sum(cell["share"] for cell in cells)
        if pop_sum != base:
            failures.append(f"{name}:{dim}: pop sum {pop_sum} != base {base}")
        if quota_sum != sample_n:
            failures.append(f"{name}:{dim}: quota sum {quota_sum} != sample_n {sample_n}")
        if cells and not math.isclose(share_sum, 1.0, rel_tol=0, abs_tol=1e-9):
            failures.append(f"{name}:{dim}: share sum {share_sum} != 1")
        for cell in cells:
            expected_share = 0.0 if base == 0 else cell["pop"] / base
            if not math.isclose(cell["share"], expected_share, rel_tol=0, abs_tol=1e-9):
                failures.append(f"{name}:{dim}:{cell['label']}: share mismatch")
    return failures


def compare_maps(name, actual_cells, expected_map):
    failures = []
    actual_map = {cell["label"]: cell["pop"] for cell in actual_cells}
    if actual_map != expected_map:
        failures.append(f"{name}: actual {actual_map} != expected {expected_map}")
    return failures


def direct_rv022u(payload):
    meta = px_meta("RV022U")
    variables = meta["variables"]
    year_var = get_var(variables, "Aasta")
    age_var = get_var(variables, "Vanuserühm")
    geo_var = get_var(variables, "Maakond")
    sex_var = get_var(variables, "Sugu")

    dims = set(payload["dimensions"])
    query_sex_values, sex_output_values = resolve_query_sex_values(sex_var, payload.get("sex_filter"))
    age_codes = select_agegroup_codes(age_var, [(band["from"], band["to"]) for band in payload.get("custom_age_groups", [])] or [(payload["age_band"]["from"], payload["age_band"]["to"])])
    geo_values = choose_geo_values(
        geo_var,
        county_filter=payload.get("county_filter"),
        for_county="county" in dims and not payload.get("county_filter"),
        for_districts="tallinn_districts" in dims,
    )

    query = []
    for var in variables:
        code = var["code"]
        if code == year_var["code"]:
            query.append({"code": code, "selection": {"filter": "item", "values": [str(payload["reference"]["year"])]}})
        elif code == age_var["code"]:
            query.append({"code": code, "selection": {"filter": "item", "values": age_codes}})
        elif code == sex_var["code"]:
            query.append({"code": code, "selection": {"filter": "item", "values": query_sex_values}})
        elif code == geo_var["code"]:
            if geo_values is None:
                continue
            query.append({"code": code, "selection": {"filter": "item", "values": geo_values}})
        else:
            query.append({"code": code, "selection": {"filter": "all", "values": ["*"]}})

    js = px_query("RV022U", query)
    ids, inv, labels, rows = parse_jsonstat(js)
    nat_pos = ids.index("Rahvus")
    sex_pos = ids.index("Sugu")
    age_pos = ids.index("Vanuserühm")
    geo_pos = ids.index("Maakond")
    nat_keys = inv["Rahvus"]
    sex_keys = inv["Sugu"]
    age_keys = inv["Vanuserühm"]
    geo_keys = inv["Maakond"]
    nat_labels = labels["Rahvus"]
    sex_labels = labels["Sugu"]
    age_labels = labels["Vanuserühm"]
    geo_labels = labels["Maakond"]

    chosen_filter = payload.get("nationality_filter", "all")
    kept = []
    for coords, value in rows:
        nat_label = nat_labels.get(nat_keys[coords[nat_pos]], nat_keys[coords[nat_pos]])
        group = map_nationality_group(nat_label)
        if group is None:
            continue
        if chosen_filter != "all" and group != chosen_filter:
            continue
        kept.append((coords, value))

    result = {
        "population_total": sum(value for _, value in kept),
        "sex": {},
        "age_group": {},
        "county": {},
        "nationality": {},
        "tallinn_districts": {},
    }

    if "sex" in dims:
        for key in sex_output_values:
            label = clean_value_text(sex_labels.get(key, key))
            result["sex"][label] = 0
        for coords, value in kept:
            key = sex_keys[coords[sex_pos]]
            if key in sex_output_values:
                label = clean_value_text(sex_labels.get(key, key))
                result["sex"][label] += value

    if "age_group" in dims:
        for key in age_keys:
            if key not in age_codes:
                continue
            label = clean_value_text(age_labels.get(key, key))
            result["age_group"][label] = 0
        for coords, value in kept:
            key = age_keys[coords[age_pos]]
            if key in age_codes:
                label = clean_value_text(age_labels.get(key, key))
                result["age_group"][label] += value

    if "county" in dims:
        if "tallinn_districts" in dims:
            result["county"]["Tallinna linn"] = sum(value for _, value in kept)
        else:
            for coords, value in kept:
                raw_label = clean_value_text(geo_labels.get(geo_keys[coords[geo_pos]], geo_keys[coords[geo_pos]]))
                if not (is_county_label(raw_label) or fold(raw_label) in city_aliases("Tallinn") or fold(raw_label) in city_aliases("Tartu")):
                    continue
                label = county_output_label(raw_label)
                result["county"][label] = result["county"].get(label, 0) + value

    if "nationality" in dims:
        ordered = [("Eestlased", "estonian"), ("Venelased", "russian"), ("Ukrainlased", "ukrainian"), ("Muud rahvused", "other")]
        for label, group in ordered:
            if chosen_filter != "all" and chosen_filter != group:
                continue
            result["nationality"][label] = 0
        for coords, value in kept:
            nat_label = nat_labels.get(nat_keys[coords[nat_pos]], nat_keys[coords[nat_pos]])
            group = map_nationality_group(nat_label)
            out_label = {
                "estonian": "Eestlased",
                "russian": "Venelased",
                "ukrainian": "Ukrainlased",
                "other": "Muud rahvused",
            }[group]
            result["nationality"][out_label] += value

    if "tallinn_districts" in dims:
        for coords, value in kept:
            raw_label = clean_value_text(geo_labels.get(geo_keys[coords[geo_pos]], geo_keys[coords[geo_pos]]))
            if not is_tallinn_district_label(raw_label):
                continue
            result["tallinn_districts"][raw_label] = result["tallinn_districts"].get(raw_label, 0) + value

    return result


def direct_rv0231u(payload):
    meta = px_meta("RV0231U")
    variables = meta["variables"]
    year_var = get_var(variables, "Aasta")
    age_var = get_var(variables, "Vanuserühm")
    geo_var = get_var(variables, "Maakond")
    sex_var = get_var(variables, "Sugu")
    edu_var = get_var(variables, "Haridustase")

    dims = set(payload["dimensions"])
    query_sex_values, sex_output_values = resolve_query_sex_values(sex_var, payload.get("sex_filter"))
    age_codes = select_agegroup_codes(age_var, [(band["from"], band["to"]) for band in payload.get("custom_age_groups", [])] or [(payload["age_band"]["from"], payload["age_band"]["to"])])
    geo_values = choose_geo_values(
        geo_var,
        county_filter=payload.get("county_filter"),
        for_county="county" in dims and not payload.get("county_filter"),
        for_districts=False,
    )

    wanted_edu = {"2", "7", "11"}
    chosen_filter = payload.get("education_filter", "all")
    if chosen_filter != "all":
        wanted_edu = {{"basic": "2", "secondary": "7", "higher": "11"}[chosen_filter]}

    query = []
    for var in variables:
        code = var["code"]
        if code == year_var["code"]:
            query.append({"code": code, "selection": {"filter": "item", "values": [str(payload["reference"]["year"])]}})
        elif code == age_var["code"]:
            query.append({"code": code, "selection": {"filter": "item", "values": age_codes}})
        elif code == sex_var["code"]:
            query.append({"code": code, "selection": {"filter": "item", "values": query_sex_values}})
        elif code == geo_var["code"]:
            if geo_values is None:
                continue
            query.append({"code": code, "selection": {"filter": "item", "values": geo_values}})
        elif code == edu_var["code"]:
            query.append({"code": code, "selection": {"filter": "item", "values": sorted(wanted_edu)}})
        else:
            query.append({"code": code, "selection": {"filter": "all", "values": ["*"]}})

    js = px_query("RV0231U", query)
    ids, inv, labels, rows = parse_jsonstat(js)
    edu_pos = ids.index("Haridustase")
    sex_pos = ids.index("Sugu")
    age_pos = ids.index("Vanuserühm")
    geo_pos = ids.index("Maakond")
    edu_keys = inv["Haridustase"]
    sex_keys = inv["Sugu"]
    age_keys = inv["Vanuserühm"]
    geo_keys = inv["Maakond"]
    edu_labels = labels["Haridustase"]
    sex_labels = labels["Sugu"]
    age_labels = labels["Vanuserühm"]
    geo_labels = labels["Maakond"]

    result = {
        "population_total": sum(value for _, value in rows),
        "sex": {},
        "age_group": {},
        "county": {},
        "education": {},
    }

    if "sex" in dims:
        for key in sex_output_values:
            result["sex"][clean_value_text(sex_labels.get(key, key))] = 0
        for coords, value in rows:
            key = sex_keys[coords[sex_pos]]
            if key in sex_output_values:
                label = clean_value_text(sex_labels.get(key, key))
                result["sex"][label] += value

    if "age_group" in dims:
        for key in age_keys:
            if key not in age_codes:
                continue
            result["age_group"][clean_value_text(age_labels.get(key, key))] = 0
        for coords, value in rows:
            key = age_keys[coords[age_pos]]
            if key in age_codes:
                label = clean_value_text(age_labels.get(key, key))
                result["age_group"][label] += value

    if "county" in dims:
        for coords, value in rows:
            raw_label = clean_value_text(geo_labels.get(geo_keys[coords[geo_pos]], geo_keys[coords[geo_pos]]))
            if not (is_county_label(raw_label) or fold(raw_label) in city_aliases("Tallinn") or fold(raw_label) in city_aliases("Tartu")):
                continue
            label = county_output_label(raw_label)
            result["county"][label] = result["county"].get(label, 0) + value

    if "education" in dims:
        for key in edu_keys:
            if key not in wanted_edu:
                continue
            result["education"][clean_value_text(edu_labels.get(key, key))] = 0
        for coords, value in rows:
            key = edu_keys[coords[edu_pos]]
            if key in wanted_edu:
                label = clean_value_text(edu_labels.get(key, key))
                result["education"][label] += value

    return result


def main():
    failures = []

    api_scenarios = [
        (
            "base_default",
            {
                "reference": {"year": 2025},
                "age_band": {"from": 16, "to": 74},
                "sample_n": 1000,
                "age_grouping_years": 10,
                "dimensions": ["sex", "age_group", "county"],
                "sex_filter": "total",
            },
        ),
        (
            "base_men_county",
            {
                "reference": {"year": 2025},
                "age_band": {"from": 18, "to": 64},
                "sample_n": 1000,
                "age_grouping_years": 5,
                "dimensions": ["sex", "age_group", "county"],
                "sex_filter": "men",
                "county_filter": "Harju maakond",
            },
        ),
        (
            "nationality_estonian_core",
            {
                "reference": {"year": 2025},
                "age_band": {"from": 16, "to": 74},
                "sample_n": 1000,
                "age_grouping_years": 10,
                "dimensions": ["sex", "age_group", "county", "nationality"],
                "sex_filter": "total",
                "nationality_filter": "estonian",
                "education_filter": "all",
            },
        ),
        (
            "nationality_tartu_city",
            {
                "reference": {"year": 2025},
                "age_band": {"from": 16, "to": 74},
                "sample_n": 1000,
                "age_grouping_years": 10,
                "dimensions": ["sex", "age_group", "county", "nationality"],
                "sex_filter": "women",
                "county_filter": "Tartu linn",
                "nationality_filter": "russian",
                "education_filter": "all",
            },
        ),
        (
            "nationality_tallinn_districts",
            {
                "reference": {"year": 2025},
                "age_band": {"from": 16, "to": 74},
                "sample_n": 1000,
                "age_grouping_years": 10,
                "dimensions": ["county", "tallinn_districts", "nationality"],
                "county_filter": "Tallinna linn",
                "nationality_filter": "estonian",
                "education_filter": "all",
                "sex_filter": "total",
            },
        ),
        (
            "education_basic_core",
            {
                "reference": {"year": 2025},
                "age_band": {"from": 16, "to": 74},
                "sample_n": 1000,
                "age_grouping_years": 10,
                "dimensions": ["sex", "age_group", "county", "education"],
                "sex_filter": "total",
                "nationality_filter": "all",
                "education_filter": "basic",
            },
        ),
        (
            "education_higher_tallinn",
            {
                "reference": {"year": 2025},
                "age_band": {"from": 16, "to": 74},
                "sample_n": 1000,
                "age_grouping_years": 10,
                "dimensions": ["sex", "age_group", "county", "education"],
                "sex_filter": "women",
                "county_filter": "Tallinna linn",
                "nationality_filter": "all",
                "education_filter": "higher",
            },
        ),
        (
            "custom_age_groups_base",
            {
                "reference": {"year": 2025},
                "age_band": {"from": 16, "to": 74},
                "sample_n": 1000,
                "age_grouping_years": 10,
                "dimensions": ["sex", "age_group"],
                "sex_filter": "total",
                "custom_age_groups": [
                    {"from": 16, "to": 24},
                    {"from": 25, "to": 44},
                    {"from": 45, "to": 64},
                    {"from": 65, "to": 74},
                ],
            },
        ),
        (
            "settlement_tartu_county",
            {
                "reference": {"year": 2025},
                "age_band": {"from": 16, "to": 74},
                "sample_n": 1000,
                "age_grouping_years": 10,
                "dimensions": ["settlement_type"],
                "county_filter": "Tartu maakond",
                "sex_filter": "total",
            },
        ),
        (
            "birth_country_county",
            {
                "reference": {"year": 2025},
                "age_band": {"from": 18, "to": 64},
                "sample_n": 1000,
                "age_grouping_years": 10,
                "dimensions": ["birth_country"],
                "county_filter": "Harju maakond",
                "sex_filter": "total",
            },
        ),
        (
            "base_women_tallinn",
            {
                "reference": {"year": 2025},
                "age_band": {"from": 20, "to": 69},
                "sample_n": 750,
                "age_grouping_years": 5,
                "dimensions": ["sex", "age_group", "county"],
                "sex_filter": "women",
                "county_filter": "Tallinna linn",
            },
        ),
        (
            "base_tartu_city",
            {
                "reference": {"year": 2025},
                "age_band": {"from": 18, "to": 74},
                "sample_n": 600,
                "age_grouping_years": 10,
                "dimensions": ["sex", "age_group", "county"],
                "sex_filter": "total",
                "county_filter": "Tartu linn",
            },
        ),
        (
            "nationality_russian_harju",
            {
                "reference": {"year": 2025},
                "age_band": {"from": 18, "to": 64},
                "sample_n": 900,
                "age_grouping_years": 5,
                "dimensions": ["sex", "age_group", "county", "nationality"],
                "sex_filter": "men",
                "county_filter": "Harju maakond",
                "nationality_filter": "russian",
                "education_filter": "all",
            },
        ),
        (
            "nationality_other_tallinn",
            {
                "reference": {"year": 2025},
                "age_band": {"from": 16, "to": 74},
                "sample_n": 850,
                "age_grouping_years": 10,
                "dimensions": ["sex", "age_group", "county", "nationality"],
                "sex_filter": "women",
                "county_filter": "Tallinna linn",
                "nationality_filter": "other",
                "education_filter": "all",
            },
        ),
        (
            "nationality_ukrainian_tallinn_districts",
            {
                "reference": {"year": 2025},
                "age_band": {"from": 16, "to": 74},
                "sample_n": 500,
                "age_grouping_years": 10,
                "dimensions": ["tallinn_districts", "nationality"],
                "county_filter": "Tallinna linn",
                "sex_filter": "total",
                "nationality_filter": "ukrainian",
                "education_filter": "all",
            },
        ),
        (
            "education_secondary_harju",
            {
                "reference": {"year": 2025},
                "age_band": {"from": 20, "to": 64},
                "sample_n": 950,
                "age_grouping_years": 5,
                "dimensions": ["sex", "age_group", "county", "education"],
                "sex_filter": "total",
                "county_filter": "Harju maakond",
                "education_filter": "secondary",
                "nationality_filter": "all",
            },
        ),
        (
            "education_higher_total",
            {
                "reference": {"year": 2025},
                "age_band": {"from": 25, "to": 74},
                "sample_n": 1000,
                "age_grouping_years": 10,
                "dimensions": ["sex", "age_group", "county", "education"],
                "sex_filter": "total",
                "education_filter": "higher",
                "nationality_filter": "all",
            },
        ),
        (
            "citizenship_country_county",
            {
                "reference": {"year": 2025},
                "age_band": {"from": 18, "to": 64},
                "sample_n": 1000,
                "age_grouping_years": 10,
                "dimensions": ["citizenship_country"],
                "county_filter": "Harju maakond",
                "sex_filter": "women",
            },
        ),
    ]

    responses = {}
    for name, payload in api_scenarios:
        response = run_api(payload)
        responses[name] = (payload, response)
        failures.extend(assert_response_invariants(name, response, payload["sample_n"]))

    # Internal consistency checks across filtered grouped outputs.
    for name in [
        "nationality_estonian_core",
        "nationality_tartu_city",
        "nationality_tallinn_districts",
        "nationality_russian_harju",
        "nationality_other_tallinn",
        "nationality_ukrainian_tallinn_districts",
        "education_basic_core",
        "education_higher_tallinn",
        "education_secondary_harju",
        "education_higher_total",
    ]:
        payload, response = responses[name]
        total = response["population_total"]
        for dim, result in response["results"].items():
            if result["base"] != total:
                failures.append(f"{name}:{dim}: base {result['base']} != population_total {total}")

    # Direct source-table comparisons.
    direct_checks = [
        ("nationality_estonian_core", direct_rv022u),
        ("nationality_tartu_city", direct_rv022u),
        ("nationality_tallinn_districts", direct_rv022u),
        ("nationality_russian_harju", direct_rv022u),
        ("nationality_other_tallinn", direct_rv022u),
        ("nationality_ukrainian_tallinn_districts", direct_rv022u),
        ("education_basic_core", direct_rv0231u),
        ("education_higher_tallinn", direct_rv0231u),
        ("education_secondary_harju", direct_rv0231u),
        ("education_higher_total", direct_rv0231u),
    ]

    for name, fn in direct_checks:
        payload, response = responses[name]
        expected = fn(payload)
        if response["population_total"] != expected["population_total"]:
            failures.append(f"{name}: population_total {response['population_total']} != expected {expected['population_total']}")
        for dim, expected_map in expected.items():
            if dim == "population_total" or dim not in response["results"]:
                continue
            failures.extend(compare_maps(name + ":" + dim, response["results"][dim]["cells"], expected_map))

    expected_error_scenarios = [
        (
            "education_tartu_city_error",
            {
                "reference": {"year": 2025},
                "age_band": {"from": 16, "to": 74},
                "sample_n": 1000,
                "age_grouping_years": 10,
                "dimensions": ["county", "education"],
                "county_filter": "Tartu linn",
                "education_filter": "basic",
                "nationality_filter": "all",
                "sex_filter": "total",
            },
            "Unknown county_filter",
        ),
        (
            "education_tallinn_districts_error",
            {
                "reference": {"year": 2025},
                "age_band": {"from": 16, "to": 74},
                "sample_n": 1000,
                "age_grouping_years": 10,
                "dimensions": ["tallinn_districts", "education"],
                "county_filter": "Tallinna linn",
                "education_filter": "basic",
                "nationality_filter": "all",
                "sex_filter": "total",
            },
            "education_filter is only possible",
        ),
        (
            "nationality_district_without_tallinn_error",
            {
                "reference": {"year": 2025},
                "age_band": {"from": 16, "to": 74},
                "sample_n": 1000,
                "age_grouping_years": 10,
                "dimensions": ["tallinn_districts", "nationality"],
                "county_filter": "Tartu linn",
                "nationality_filter": "estonian",
                "education_filter": "all",
                "sex_filter": "total",
            },
            "Tallinn Districts with nationality_filter is only possible",
        ),
    ]

    for name, payload, expected_snippet in expected_error_scenarios:
        did_error, error_text = run_api_expect_error(payload)
        if not did_error:
            failures.append(f"{name}: expected error but request succeeded")
            continue
        if expected_snippet not in error_text:
            failures.append(f"{name}: expected error containing {expected_snippet!r}, got {error_text!r}")

    nationality_matrix_count = 0
    for county_filter in ["Harju maakond", "Tallinna linn", "Tartu linn", "Ida-Viru maakond"]:
        for nationality_filter in ["estonian", "russian", "ukrainian", "other"]:
            for sex_filter in ["total", "men", "women"]:
                payload = {
                    "reference": {"year": 2025},
                    "age_band": {"from": 16, "to": 74},
                    "sample_n": 400,
                    "age_grouping_years": 10,
                    "dimensions": ["sex", "age_group", "county", "nationality"],
                    "county_filter": county_filter,
                    "nationality_filter": nationality_filter,
                    "education_filter": "all",
                    "sex_filter": sex_filter,
                }
                name = f"matrix_nat_{county_filter}_{nationality_filter}_{sex_filter}"
                response = run_api(payload)
                failures.extend(assert_response_invariants(name, response, payload["sample_n"]))
                total = response["population_total"]
                for dim, result in response["results"].items():
                    if result["base"] != total:
                        failures.append(f"{name}:{dim}: base {result['base']} != population_total {total}")
                nationality_matrix_count += 1

    education_matrix_count = 0
    for county_filter in ["Harju maakond", "Tallinna linn", "Tartu maakond", "Võru maakond"]:
        for education_filter in ["basic", "secondary", "higher"]:
            for sex_filter in ["total", "women"]:
                payload = {
                    "reference": {"year": 2025},
                    "age_band": {"from": 20, "to": 74},
                    "sample_n": 400,
                    "age_grouping_years": 5,
                    "dimensions": ["sex", "age_group", "county", "education"],
                    "county_filter": county_filter,
                    "nationality_filter": "all",
                    "education_filter": education_filter,
                    "sex_filter": sex_filter,
                }
                name = f"matrix_edu_{county_filter}_{education_filter}_{sex_filter}"
                response = run_api(payload)
                failures.extend(assert_response_invariants(name, response, payload["sample_n"]))
                total = response["population_total"]
                for dim, result in response["results"].items():
                    if result["base"] != total:
                        failures.append(f"{name}:{dim}: base {result['base']} != population_total {total}")
                education_matrix_count += 1

    tallinn_district_matrix_count = 0
    for nationality_filter in ["estonian", "russian", "ukrainian", "other"]:
        payload = {
            "reference": {"year": 2025},
            "age_band": {"from": 16, "to": 74},
            "sample_n": 300,
            "age_grouping_years": 10,
            "dimensions": ["county", "tallinn_districts", "nationality"],
            "county_filter": "Tallinna linn",
            "nationality_filter": nationality_filter,
            "education_filter": "all",
            "sex_filter": "total",
        }
        name = f"matrix_tallinn_districts_{nationality_filter}"
        response = run_api(payload)
        failures.extend(assert_response_invariants(name, response, payload["sample_n"]))
        district_sum = sum(cell["pop"] for cell in response["results"]["tallinn_districts"]["cells"])
        county_sum = sum(cell["pop"] for cell in response["results"]["county"]["cells"])
        if district_sum != county_sum or district_sum != response["population_total"]:
            failures.append(f"{name}: district sum {district_sum}, county sum {county_sum}, total {response['population_total']}")
        if len(response["results"]["tallinn_districts"]["cells"]) != 8:
            failures.append(f"{name}: expected 8 Tallinn districts, got {len(response['results']['tallinn_districts']['cells'])}")
        tallinn_district_matrix_count += 1

    county_option_labels = [item["label"] for item in http_get_json(f"{API_BASE}/v1/options/counties")["items"]]
    county_option_count = len(county_option_labels)
    required_labels = {"Tallinna linn", "Tartu linn", "Harju Maakond", "Tartu Maakond"}
    missing_labels = sorted(label for label in required_labels if label not in county_option_labels)
    if missing_labels:
        failures.append(f"county_options: missing {missing_labels}")

    if failures:
        print("FAILURES")
        for failure in failures:
            print(failure)
        return 1

    print("ALL TESTS PASSED")
    print(f"Scenarios checked: {len(api_scenarios)}")
    print(f"Direct source cross-checks: {len(direct_checks)}")
    print(f"Expected error scenarios: {len(expected_error_scenarios)}")
    print(f"Nationality matrix scenarios: {nationality_matrix_count}")
    print(f"Education matrix scenarios: {education_matrix_count}")
    print(f"Tallinn district matrix scenarios: {tallinn_district_matrix_count}")
    print(f"County options checked: {county_option_count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
