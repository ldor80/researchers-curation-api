# server.py
import os, csv, io, re, json, datetime
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field
from fastapi.responses import JSONResponse

API_KEY = os.getenv("ACTIONS_API_KEY")  # set this before running

app = FastAPI(title="People Curation Actions API", version="1.0.0")

HTTPS_RX = re.compile(r"https://[^\s\[\]\(\)\"]+")
CTG_RX   = re.compile(r"(NCT\d{8})")

def last_https_token(text: str) -> Optional[str]:
    if not isinstance(text, str):
        return None
    matches = HTTPS_RX.findall(text)
    return matches[-1] if matches else None

def purify_url(url: str) -> Optional[str]:
    token = last_https_token(url)
    if not token: 
        return None
    # strip tracking/fragments
    for bad in ["utm_", "gclid", "fbclid", "#:~:text="]:
        if bad in token:
            token = token.split("?")[0].split("#")[0]
    # normalize ClinicalTrials show → study
    token = token.replace("/ct2/show/", "/study/")
    return token

def preclean_people_obj(obj: Dict[str, Any]) -> Dict[str, Any]:
    """Field-aware preclean: keep ONLY plain https tokens in URL fields/arrays,
       repair contacts, normalize ClinicalTrials source_urls."""
    def clean_contacts(contacts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out = []
        for c in contacts or []:
            label = c.get("label")
            ctype = c.get("type")
            url   = c.get("url")
            vd    = c.get("verified_date")
            if ctype == "email" and isinstance(url, str) and url.startswith("mailto:"):
                email = url[7:]
                if re.fullmatch(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", email):
                    out.append({"label": label, "type": "email", "url": url, "verified_date": vd})
                continue
            if ctype == "phone" and isinstance(url, str) and url.startswith("tel:"):
                out.append({"label": label, "type": "phone", "url": url, "verified_date": vd})
                continue
            # page: extract last https token
            pu = purify_url(url or "")
            if pu:
                out.append({"label": label, "type": "page", "url": pu, "verified_date": vd})
        return out

    def clean_evidence(evs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out = []
        for e in evs or []:
            ce = dict(e)
            cu = purify_url(ce.get("canonical_url", "") or "")
            pu = purify_url(ce.get("pdf_url", "") or "") if ce.get("pdf_url") else None
            if cu:
                # medRxiv/bioRxiv must be DOI landing if we can detect one
                if "medrxiv.org" in cu or "biorxiv.org" in cu:
                    doi = last_https_token(ce.get("canonical_url", ""))
                    if doi and "10.1101" in doi:
                        cu = doi
                ce["canonical_url"] = cu
                if pu: ce["pdf_url"] = pu
                out.append(ce)
        return out

    def clean_trials(trials: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out = []
        for t in trials or []:
            ct = dict(t)
            # normalize nct_id from either url or provided field
            nct = ct.get("nct_id")
            if not nct:
                # try to derive from source_urls
                for u in (ct.get("source_urls") or []):
                    m = CTG_RX.search(u or "")
                    if m: nct = m.group(1); break
            if nct:
                ct["nct_id"] = nct
            # purify source_urls
            srcs = []
            for u in (ct.get("source_urls") or []):
                pu = purify_url(u)
                if pu: srcs.append(pu)
            ct["source_urls"] = srcs or ([f"https://clinicaltrials.gov/study/{nct}"] if nct else [])
            out.append(ct)
        return out

    people = obj.get("people") or []
    for p in people:
        # URL-like fields at top level
        for kl in ("key_links",):
            if p.get(kl):
                cleaned = []
                for link in p[kl]:
                    if not isinstance(link, dict): 
                        continue
                    pu = purify_url(link.get("url", ""))
                    if pu:
                        cleaned.append({"label": link.get("label"), "url": pu})
                p[kl] = cleaned

        p["contacts"] = clean_contacts(p.get("contacts") or [])
        p["evidence"] = clean_evidence(p.get("evidence") or [])
        p["trials"]   = clean_trials(p.get("trials") or [])
    obj["people"] = people
    return obj

def validate_people_obj(obj: Dict[str, Any]) -> Dict[str, Any]:
    errors, warnings = [], []
    if not isinstance(obj, dict):
        return {"errors": ["Top-level must be JSON object"], "warnings": warnings}

    people = obj.get("people")
    if not isinstance(people, list) or not people:
        errors.append("`people` must be a non-empty array.")

    # people_count check
    if isinstance(people, list):
        pc = obj.get("people_count")
        if pc is not None and pc != len(people):
            warnings.append(f"`people_count` != len(people) ({pc} vs {len(people)}). Updated automatically.")
            obj["people_count"] = len(people)

        # id uniqueness + original_order contiguous
        ids = []
        oo  = []
        for i, p in enumerate(people, start=1):
            pid = p.get("id")
            if not pid: errors.append(f"person[{i}]: missing id")
            else: ids.append(pid)
            oo.append(p.get("original_order"))
            # summary length (soft)
            st = p.get("summary_text") or ""
            wc = len(re.findall(r"\w+", st))
            if wc < 140 or wc > 220:
                warnings.append(f"person[{i}]/{pid}: summary_text words={wc} (expected 140–220)")
            # URL hygiene (hard)
            for field in ("contacts",):
                for c in (p.get(field) or []):
                    u = c.get("url")
                    if isinstance(u, str):
                        if not (u.startswith("https://") or u.startswith("mailto:") or u.startswith("tel:")):
                            errors.append(f"person[{i}]/{pid}/{field}: bad url `{u}`")
                    else:
                        errors.append(f"person[{i}]/{pid}/{field}: url missing")
            for ev in (p.get("evidence") or []):
                cu = ev.get("canonical_url")
                if not isinstance(cu, str) or not cu.startswith("https://"):
                    errors.append(f"person[{i}]/{pid}/evidence: canonical_url invalid")
        if len(ids) != len(set(ids)):
            errors.append("Duplicate `id` values found.")
        if sorted(oo) != list(range(1, len(people)+1)):
            warnings.append("`original_order` not contiguous 1..N (will not auto-fix here).")

    return {"errors": errors, "warnings": warnings}

def make_csv(obj: Dict[str, Any]) -> str:
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["full_name","section","role","primary_affiliation","country","pins","score_total","contact_labels","trial_ncts"])
    for p in obj.get("people", []):
        aff = ""
        if p.get("affiliations"):
            a0 = p["affiliations"][0]
            aff = a0.get("name","")
            country = a0.get("country","")
        else:
            country = ""
        pins = ";".join(p.get("pins") or [])
        score = (p.get("score_breakdown") or {}).get("total", "")
        labels = ";".join(c.get("label","") for c in (p.get("contacts") or []))
        ncts = ";".join(t.get("nct_id","") for t in (p.get("trials") or []))
        w.writerow([p.get("full_name",""), p.get("section",""), p.get("role",""), aff, country, pins, score, labels, ncts])
    return out.getvalue()

def check_key(x_api_key: Optional[str]):
    if not API_KEY:
        return  # run open server if you really want (not recommended)
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

class EmitRequest(BaseModel):
    payload: Dict[str, Any] = Field(..., description="The people JSON object produced by the generator.")

class EmitResponse(BaseModel):
    status: str
    cleaned_json: Optional[Dict[str, Any]] = None
    csv_base64: Optional[str] = None
    errors: List[str] = []
    warnings: List[str] = []

@app.post("/emit_people_json", response_model=EmitResponse, summary="Validate + preclean + return cleaned JSON/CSV")
def emit_people_json(req: EmitRequest, x_api_key: Optional[str] = Header(None, convert_underscores=False)):
    check_key(x_api_key)
    # 1) preclean
    pre = preclean_people_obj(req.payload)
    # 2) validate
    val = validate_people_obj(pre)
    errors, warnings = val["errors"], val["warnings"]
    status = "pass" if not errors else "fail"
    resp = {"status": status, "cleaned_json": pre if status=="pass" else None, "errors": errors, "warnings": warnings}
    # 3) CSV (only if pass)
    if status == "pass":
        import base64
        csv_text = make_csv(pre)
        resp["csv_base64"] = base64.b64encode(csv_text.encode("utf-8")).decode("ascii")
    return JSONResponse(resp)

class PurifyRequest(BaseModel):
    url: str

class PurifyResponse(BaseModel):
    purified_url: Optional[str]
    ok: bool

@app.post("/purify_url", response_model=PurifyResponse, summary="Purify a single URL (strip tracking/markdown, normalize)")
def purify_url_action(req: PurifyRequest, x_api_key: Optional[str] = Header(None, convert_underscores=False)):
    check_key(x_api_key)
    pu = purify_url(req.url)
    return {"purified_url": pu, "ok": bool(pu)}

# Health
@app.get("/healthz")
def healthz(): return {"ok": True, "ts": datetime.datetime.utcnow().isoformat()}
