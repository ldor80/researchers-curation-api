#!/usr/bin/env python3
import json, sys, re, os
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from datetime import date

ALLOWED_SECTIONS = {
    "Care & Management", "Trials & Translational",
    "Models & Assays", "Registries & Biobanks"
}
ALLOWED_TAGS = {
    "peer_reviewed","preprint","trial_registry","case_series",
    "review_consensus","dataset_protocol","news_talk",
    "preclinical_rescue_in_vitro","preclinical_rescue_in_vivo","patent_grant"
}
URL_RE = re.compile(r"^https://[^\s\[\]\(\)]+$")
MAILTO_RE = re.compile(r"^mailto:([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})$")
TEL_RE = re.compile(r"^tel:\+?[0-9\-\(\)\s]+$")
TRACK_KEYS = {"utm_source","utm_medium","utm_campaign","utm_term","utm_content","gclid","fbclid"}

def unwrap_markdown(u: str) -> str:
    m = re.match(r"^\[[^\]]+\]\((https?://[^)]+)\)$", u.strip())
    return m.group(1) if m else u

def strip_tracking(u: str) -> str:
    try:
        p = urlparse(u)
        if p.scheme not in ("http","https"): return u
        q = [(k,v) for k,v in parse_qsl(p.query, keep_blank_values=True) if k not in TRACK_KEYS]
        frag = "" if p.fragment.startswith(":~:text=") else p.fragment
        return urlunparse(( "https", p.netloc, p.path, p.params, urlencode(q, doseq=True), frag ))
    except Exception:
        return u

def normalize_ctgov(u: str) -> str:
    if "clinicaltrials.gov" in u and "/ct2/show/" in u:
        nct = u.split("/ct2/show/")[-1].split("?")[0].split("#")[0]
        return f"https://clinicaltrials.gov/study/{nct}"
    return u

def clean_url(u: str) -> str:
    u = unwrap_markdown(u)
    u = strip_tracking(u)
    u = normalize_ctgov(u)
    return u

def word_count(s: str) -> int:
    return len(re.findall(r"\b\w+\b", s or ""))

def fail(msg, errors): errors.append(msg)

def lint(obj):
    errors, fixes, warns = [], [], []
    # top-level sanity
    if not isinstance(obj, dict): fail("Top-level JSON is not an object.", errors); return errors, fixes, warns, None
    people = obj.get("people", [])
    if not isinstance(people, list): fail("`people` must be an array.", errors)
    # derive people_count
    obj["people_count"] = len(people)
    # ids and original_order
    ids = set()
    orders = []
    for i,p in enumerate(people, start=1):
        if "id" not in p: fail(f"person[{i}] missing id", errors)
        else:
            if p["id"] in ids: fail(f"duplicate id: {p['id']}", errors)
            ids.add(p["id"])
        if "original_order" not in p: p["original_order"] = i; fixes.append(f"set original_order for {p.get('id','?')}")
        orders.append(p["original_order"])
    if sorted(orders) != list(range(1, len(people)+1)):
        fail("original_order is not contiguous 1..N.", errors)

    today = date.today().isoformat()

    def check_url(u, what, context):
        u2 = clean_url(u)
        if u2 != u: fixes.append(f"{context}: normalized URL")
        if not URL_RE.match(u2):
            fail(f"{context}: invalid URL `{u2}` for {what}", errors)
        if any(tok in u2 for tok in ("[","]","(",")"," ")):
            fail(f"{context}: URL contains illegal chars for {what}", errors)
        return u2

    for i,p in enumerate(people, start=1):
        ctx = f"person[{i}]/{p.get('id','?')}"
        # section
        sec = p.get("section","").split(" | ")[0] if " | " in p.get("section","") else p.get("section","")
        if sec not in ALLOWED_SECTIONS:
            fail(f"{ctx}: section `{p.get('section')}` not in enum", errors)
        # summary length
        wc = word_count(p.get("summary_text",""))
        if wc < 140 or wc > 220: warns.append(f"{ctx}: summary_text words={wc} (expected 140–220)")
        # evidence
        ev = p.get("evidence", [])
        if not isinstance(ev, list) or not ev:
            fail(f"{ctx}: missing evidence array", errors)
        for j,e in enumerate(ev, start=1):
            if e.get("tag") not in ALLOWED_TAGS:
                fail(f"{ctx}: evidence[{j}] invalid tag `{e.get('tag')}`", errors)
            url = e.get("canonical_url","")
            if not url: fail(f"{ctx}: evidence[{j}] missing canonical_url", errors)
            else:
                url2 = check_url(url, "canonical_url", f"{ctx}/evidence[{j}]")
                e["canonical_url"] = url2
                if "biorxiv.org" in url2 or "medrxiv.org" in url2:
                    if not url2.startswith("https://doi.org/10.1101/"):
                        warns.append(f"{ctx}/evidence[{j}]: preprint should use DOI landing")
            if "pdf_url" in e and e["pdf_url"]:
                e["pdf_url"] = check_url(e["pdf_url"], "pdf_url", f"{ctx}/evidence[{j}]")
        # contacts
        for j,c in enumerate(p.get("contacts", []), start=1):
            typ = c.get("type")
            url = c.get("url","")
            if typ == "email":
                m = MAILTO_RE.match(url)
                if not m: fail(f"{ctx}/contacts[{j}]: invalid mailto", errors)
            elif typ == "phone":
                if not TEL_RE.match(url): fail(f"{ctx}/contacts[{j}]: invalid tel:", errors)
            else:
                c["type"] = "page"
                c["url"] = check_url(url, "page", f"{ctx}/contacts[{j}]")
            # verified date
            if "verified_date" not in c or not c["verified_date"]:
                warns.append(f"{ctx}/contacts[{j}]: missing verified_date (set to today)")
                c["verified_date"] = today
        # key_links
        kl = p.get("key_links",[])
        for j,k in enumerate(kl, start=1):
            k["url"] = check_url(k.get("url",""), "key_link", f"{ctx}/key_links[{j}]")
        # trials
        for j,t in enumerate(p.get("trials",[]), start=1):
            su = t.get("source_urls",[])
            if not isinstance(su, list) or not su:
                fail(f"{ctx}/trials[{j}]: source_urls must be non-empty list", errors)
            for k,u in enumerate(su, start=1):
                t["source_urls"][k-1] = check_url(u, "trial source", f"{ctx}/trials[{j}]")

    # top-level omitted
    om = obj.get("omitted_candidates", [])
    if not isinstance(om, list):
        fail("`omitted_candidates` must be an array.", errors)

    status = "pass" if not errors else "fail"
    return errors, fixes, warns, status

def main():
    if len(sys.argv) < 3:
        print("Usage: people_json_linter.py INPUT.json OUTPUT.json", file=sys.stderr)
        sys.exit(2)
    src, dst = sys.argv[1], sys.argv[2]
    with open(src, "r", encoding="utf-8") as f:
        raw = f.read().strip()
    try:
        obj = json.loads(raw)
    except Exception as e:
        print(f"FATAL: not valid JSON: {e}", file=sys.stderr)
        sys.exit(1)
    errors, fixes, warns, status = lint(obj)
    report = {
        "status": status, "errors": errors, "fixes": fixes, "warnings": warns,
        "people_count": obj.get("people_count", None)
    }
    print(json.dumps(report, indent=2))
    if status == "pass":
        with open(dst, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()
