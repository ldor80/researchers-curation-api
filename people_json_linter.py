#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
people_json_linter.py

Adds robust pre-clean + JSON extraction so pasted GPT outputs with wrappers
(```json fences, BEGIN/END JSON lines, stray prose, curly quotes, trailing commas)
can still be parsed deterministically.

On PASS -> writes cleaned JSON (--out) and optional CSV (--csv).
On FAIL  -> prints machine-readable report to stdout and exits 1 (no files written).
"""

import argparse, json, re, sys, csv
from copy import deepcopy
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

# ---------------- Config / Policy ----------------

URL_REGEX   = re.compile(r'^https://[^\s\[\]\(\)]+$')
EMAIL_REGEX = re.compile(r'^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$')
DATE_REGEX  = re.compile(r'^\d{4}-\d{2}-\d{2}$')
PREPRINT_CONTENT_RE = re.compile(r'https?://(?:www\.)?(?:bio|med)rxiv\.org/content/(10\.1101/[^/\s#?]+)(?:v\d+)?(?:/full\.pdf)?', re.I)

ALLOWED_SECTIONS = {
    "Care & Management", "Trials & Translational", "Models & Assays", "Registries & Biobanks"
}
ALLOWED_TAGS = {
    "peer_reviewed", "preprint", "trial_registry", "case_series",
    "review_consensus", "dataset_protocol", "news_talk",
    "preclinical_rescue_in_vitro", "preclinical_rescue_in_vivo", "patent_grant"
}

CTG_SHOW_PAT = re.compile(r'^https://clinicaltrials\.gov/ct2/show/(NCT[0-9]{8})$', re.I)
TRACKING_PARAMS_PREFIXES = ('utm_', 'gclid', 'fbclid', 'mc_cid', 'mc_eid', 'igshid', 'ref')

# ---------------- Text pre-clean helpers ----------------

def strip_bom(text: str) -> str:
    return text.lstrip('\ufeff')

def strip_wrappers(text: str) -> str:
    # Remove backtick code fences and “BEGIN/END JSON” markers
    lines = []
    for line in text.splitlines():
        l = line.strip()
        if l.startswith('```'):
            continue
        if l.upper() in ('BEGIN JSON','END JSON','BEGIN MARKDOWN','END MARKDOWN'):
            continue
        lines.append(line)
    return "\n".join(lines)

def replace_curly_quotes(text: str) -> str:
    # Replace curly quotes with straight quotes
    return (text
            .replace('\u201c','"').replace('\u201d','"')
            .replace('\u2018',"'").replace('\u2019',"'"))

def preclean_markdown_links(text: str) -> str:
    # [label](https://...) -> https://...
    text = re.sub(r'\[([^\]]+)\]\((https?://[^\s)]+)\)', r'\2', text)
    # [https://host/...] -> https://host/...
    text = re.sub(r'\[(https?://[^\]\s)]+)\]', r'\1', text)
    # Fix accidental leading '[' in quoted URLs
    text = re.sub(r'"\[https://', r'"https://', text)
    # Remove lingering "](https://...)" glue
    text = text.replace("](", "")
    text = text.replace(")]", ")")
    return text

def normalize_ctgov_in_text(text: str) -> str:
    return re.sub(r'https://clinicaltrials\.gov/ct2/show/(NCT[0-9]{8})',
                  r'https://clinicaltrials.gov/study/\1', text)

def http_to_https(text: str) -> str:
    return re.sub(r'"http://', '"https://', text)

def strip_trailing_commas(text: str) -> str:
    # Last-resort: remove trailing commas before closing } or ]
    # Safe enough for JSON; do not overuse.
    return re.sub(r',(\s*[}\]])', r'\1', text)

def extract_first_json_object(text: str) -> str:
    """
    Extract the first top-level JSON object {...} using a brace-aware scanner.
    Ignores braces inside strings and escaped quotes.
    Returns the substring if found, else ''.
    """
    s = text
    start = s.find('{')
    if start == -1:
        return ''
    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(s)):
        ch = s[i]
        if in_str:
            if esc:
                esc = False
            elif ch == '\\':
                esc = True
            elif ch == '"':
                in_str = False
        else:
            if ch == '"':
                in_str = True
            elif ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    return s[start:i+1]
    return ''  # not balanced

def preclean_text(raw: str, aggressive: bool=False) -> str:
    """
    Mechanical hygiene BEFORE JSON parse.
    If aggressive=True, also try curly→straight quotes and trailing comma removal,
    and extract first top-level object by brace scan.
    """
    t = strip_bom(raw)
    t = strip_wrappers(t)
    t = preclean_markdown_links(t)
    t = normalize_ctgov_in_text(t)
    t = http_to_https(t)

    if aggressive:
        t = replace_curly_quotes(t)
        # If multiple objects or prose present, extract the first top-level object
        candidate = extract_first_json_object(t)
        if candidate:
            t = candidate
        # Try removing trailing commas (only in aggressive mode)
        t = strip_trailing_commas(t)

    return t

# ---------------- URL utilities ----------------

def purify_url(u: str) -> str:
    if not isinstance(u, str):
        return u
    if u.startswith('http://'):
        u = 'https://' + u[len('http://'):]
    if not u.startswith('https://'):
        return u
    parts = urlsplit(u)
    q = [(k, v) for (k, v) in parse_qsl(parts.query, keep_blank_values=True)
         if not any(k.lower().startswith(p) for p in TRACKING_PARAMS_PREFIXES)]
    query = urlencode(q, doseq=True)
    frag = ''
    purified = urlunsplit((parts.scheme, parts.netloc, parts.path, query, frag))
    purified = purified.rstrip(')]')
    return purified

def medrxiv_to_doi(url: str) -> str:
    if not isinstance(url, str):
        return url
    m = PREPRINT_CONTENT_RE.search(url)
    if not m:
        return url
    return f'https://doi.org/{m.group(1)}'

def normalize_ctgov(url: str) -> str:
    if not isinstance(url, str):
        return url
    m = CTG_SHOW_PAT.match(url)
    if m:
        return f'https://clinicaltrials.gov/study/{m.group(1)}'
    return url

def extract_all_urls(s: str):
    return re.findall(r'https://[^\s\[\]\(\)"]+', s or "")

def word_count(s: str) -> int:
    return len((s or "").strip().split())

# ---------------- Structure-level cleanup ----------------

def clean_person(person, errors, warnings, idx):
    pid = person.get('id', f'?{idx}')
    # Section
    if person.get("section") not in ALLOWED_SECTIONS:
        errors.append(f"person[{idx}]/{pid}: invalid section '{person.get('section')}'")

    # Summary length
    sw = word_count(person.get("summary_text",""))
    if sw < 140 or sw > 220:
        warnings.append(f"person[{idx}]/{pid}: summary_text words={sw} (expected 140–220)")

    # Evidence
    ev = person.get("evidence", [])
    for j, e in enumerate(ev):
        tag = e.get("tag")
        if tag not in ALLOWED_TAGS:
            errors.append(f"person[{idx}]/{pid}/evidence[{j}]: invalid tag '{tag}'")

        cu = e.get("canonical_url")
        if isinstance(cu, str):
            cu = purify_url(cu)
            cu = normalize_ctgov(cu)
            if tag == "preprint":
                cu = medrxiv_to_doi(cu)
            e["canonical_url"] = cu

        pdf = e.get("pdf_url")
        if isinstance(pdf, str):
            e["pdf_url"] = purify_url(pdf)

        cu_ok = isinstance(e.get("canonical_url"), str) and URL_REGEX.match(e["canonical_url"])
        if not cu_ok:
            errors.append(f"person[{idx}]/{pid}/evidence[{j}]: invalid canonical_url '{e.get('canonical_url')}'")

    # Contacts
    contacts = person.get("contacts", [])
    fixed_contacts = []
    for j, c in enumerate(contacts):
        ctype = c.get("type","page")
        url = c.get("url","")
        vdate = c.get("verified_date","")

        # unwrap debris / pick first https
        if isinstance(url, str) and ("](" in url or "[" in url or "]" in url or "(" in url or ")" in url or not URL_REGEX.match(url.replace("mailto:","https://").replace("tel:","https://"))):
            urls = extract_all_urls(url)
            if urls:
                url = urls[0]
        if isinstance(url, str):
            url = purify_url(url)
            url = normalize_ctgov(url)

        if ctype == "email":
            if url.startswith("mailto:"):
                email = url[len("mailto:"):]
            else:
                if EMAIL_REGEX.match(url):
                    url = f"mailto:{url}"
                    email = url[len("mailto:"):]
                else:
                    email = url
            if not EMAIL_REGEX.match(email):
                errors.append(f"person[{idx}]/{pid}/contacts[{j}]: invalid mailto")
        elif ctype == "phone":
            if not isinstance(url, str) or not url.startswith("tel:"):
                errors.append(f"person[{idx}]/{pid}/contacts[{j}]: invalid phone URL (must start 'tel:')")
        else:
            if not isinstance(url, str) or not URL_REGEX.match(url):
                errors.append(f"person[{idx}]/{pid}/contacts[{j}]: invalid URL '{url}' for page")

        if not isinstance(vdate, str) or not DATE_REGEX.match(vdate):
            warnings.append(f"person[{idx}]/{pid}/contacts[{j}]: missing or non-ISO verified_date")

        c["url"] = url
        fixed_contacts.append(c)
    person["contacts"] = fixed_contacts

    # Key links
    for j, k in enumerate(person.get("key_links", [])):
        if isinstance(k.get("url"), str):
            k["url"] = purify_url(k["url"])
            k["url"] = normalize_ctgov(k["url"])
            if not URL_REGEX.match(k["url"]):
                errors.append(f"person[{idx}]/{pid}/key_links[{j}]: invalid url '{k['url']}'")

    # Trials
    trials = person.get("trials", [])
    for j, t in enumerate(trials):
        su = t.get("source_urls")
        urls = []
        if isinstance(su, list):
            urls = su
        elif isinstance(su, str):
            urls = extract_all_urls(su)
        else:
            errors.append(f"person[{idx}]/{pid}/trials[{j}]: source_urls must be an array or string with https URLs")

        clean_urls = []
        for u in urls:
            u = purify_url(u)
            u = normalize_ctgov(u)
            if URL_REGEX.match(u):
                clean_urls.append(u)
        if not clean_urls:
            errors.append(f"person[{idx}]/{pid}/trials[{j}]: no valid https URLs in source_urls")
        t["source_urls"] = clean_urls

        if isinstance(t.get("nct_id"), str):
            t["nct_id"] = t["nct_id"].upper()

    return person

# ---------------- CSV pivot ----------------

def write_csv(people, csv_path):
    headers = ["full_name","section","role","primary_affiliation","country","pins","score_total","contact_labels","trial_ncts"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for p in people:
            prim_aff, country = "", ""
            for a in p.get("affiliations", []):
                if a.get("type") == "Primary":
                    prim_aff = a.get("name",""); country = a.get("country",""); break
            pins = ";".join(p.get("pins", []) or [])
            score_total = (p.get("score_breakdown") or {}).get("total", "")
            contact_labels = ";".join([c.get("label","") for c in p.get("contacts", [])])
            trial_ncts = ";".join([t.get("nct_id","") for t in p.get("trials", [])])
            w.writerow([p.get("full_name",""), p.get("section",""), p.get("role",""),
                        prim_aff, country, pins, score_total, contact_labels, trial_ncts])

# ---------------- Main ----------------

def main():
    ap = argparse.ArgumentParser(description="Pre-clean, parse, and lint People JSON")
    ap.add_argument("input", help="Input file (raw generator paste or JSON)")
    ap.add_argument("--out", required=True, help="Path to write cleaned JSON on PASS")
    ap.add_argument("--csv", help="Optional path to write CSV pivot on PASS")
    ap.add_argument("--preclean", action="store_true", help="Run pre-clean before validation (recommended)")
    args = ap.parse_args()

    # Read raw
    try:
        raw = open(args.input, "r", encoding="utf-8").read()
    except FileNotFoundError as e:
        print(json.dumps({"status":"fail","errors":[f"File not found: {args.input}"],"warnings":[]}, ensure_ascii=False, indent=2))
        sys.exit(1)

    # Pass 1: normal preclean if requested
    text = raw
    if args.preclean:
        text = preclean_text(raw, aggressive=False)

    # Try parse
    try:
        data = json.loads(text)
    except json.JSONDecodeError as e1:
        # Pass 2: aggressive preclean + extraction
        text2 = preclean_text(raw, aggressive=True)
        try:
            data = json.loads(text2)
        except json.JSONDecodeError as e2:
            # Final fail with context
            snippet = text2[max(0, e2.pos-60): e2.pos+60]
            report = {
                "status": "fail",
                "errors": [f"JSON parse failed even after --preclean (aggressive). line={e2.lineno} col={e2.colno}",
                           f"around: {snippet}"],
                "warnings": []
            }
            print(json.dumps(report, ensure_ascii=False, indent=2))
            sys.exit(1)

    errors, warnings = [], []
    cleaned = deepcopy(data)

    # Schema presence
    if not isinstance(cleaned.get("people"), list):
        errors.append("Top-level 'people' must be an array")

    # People loop
    people = cleaned.get("people") or []
    for i, person in enumerate(people, start=1):
        if "original_order" not in person or not isinstance(person["original_order"], int):
            person["original_order"] = i
        clean_person(person, errors, warnings, i)

    # Counts/IDs normalization
    cleaned["people_count"] = len(people)
    oo = [p.get("original_order") for p in people]
    if sorted(oo) != list(range(1, len(people)+1)):
        for idx, p in enumerate(people, start=1):
            p["original_order"] = idx

    status = "pass" if not errors else "fail"
    report = {"status": status, "errors": errors, "warnings": warnings, "people_count": cleaned.get("people_count", 0)}

    if status == "fail":
        print(json.dumps(report, ensure_ascii=False, indent=2))
        sys.exit(1)

    # Write cleaned JSON
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=2)

    if args.csv:
        write_csv(cleaned.get("people", []), args.csv)

    print(json.dumps(report, ensure_ascii=False, indent=2))
    sys.exit(0)

if __name__ == "__main__":
    main()
