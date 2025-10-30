#!/usr/bin/env python3
import sys, json, requests

TERM = sys.argv[1] if len(sys.argv) > 1 else "STXBP1"

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "local-ctgov-test/1.0 (+you@example.com)"
}

def try_v2(term):
    url = "https://clinicaltrials.gov/api/v2/studies"
    params = {
        "query.term": term,
        "pageSize": 20,
        "countTotal": "true",
        "format": "json"
    }
    r = requests.get(url, params=params, headers=HEADERS, timeout=30)
    return r.status_code, r.text, (r.json() if r.headers.get("content-type","").startswith("application/json") else None)

def try_v1(term):
    url = "https://clinicaltrials.gov/api/query/study_fields"
    params = {
        "expr": term,
        "fields": "NCTId,OfficialTitle,OverallStatus,Phase,InterventionName,LocationFacility",
        "min_rnk": 1,
        "max_rnk": 20,
        "fmt": "json"
    }
    r = requests.get(url, params=params, timeout=30)
    return r.status_code, r.text, (r.json() if r.headers.get("content-type","").startswith("application/json") else None)

def summarize_v2(js):
    n = js.get("totalCount")
    ids = []
    for s in js.get("studies", []):
        try:
            ids.append(s["protocolSection"]["identificationModule"]["nctId"])
        except Exception:
            pass
    return n, ids

def summarize_v1(js):
    root = js.get("StudyFieldsResponse", {})
    n = root.get("NStudiesFound")
    ids = []
    for row in root.get("StudyFields", []):
        if row.get("NCTId"):
            ids.append(row["NCTId"][0])
    return n, ids

print(f"Query: {TERM}\n")

# ---- Try v2 first
s, raw, js = try_v2(TERM)
print(f"[v2] HTTP {s}")
if s == 200 and js:
    n, ids = summarize_v2(js)
    print(f"[v2] totalCount={n}, first IDs={ids[:5]}")
else:
    print(f"[v2] body (truncated): {raw[:300]}")

# ---- Then v1 fallback
s1, raw1, js1 = try_v1(TERM)
print(f"\n[v1] HTTP {s1}")
if s1 == 200 and js1:
    n1, ids1 = summarize_v1(js1)
    print(f"[v1] NStudiesFound={n1}, first IDs={ids1[:5]}")
else:
    print(f"[v1] body (truncated): {raw1[:300]}")

print("\nDone.")
