import base64
import io
import os
import re
import time
from typing import List, Optional, Tuple
from urllib.parse import urlparse

import requests
from fastapi import FastAPI, Header, HTTPException
from openpyxl import Workbook
from pydantic import BaseModel

APP_API_KEY = os.getenv("APP_API_KEY", "")
SERPAPI_KEY = os.getenv("SERPAPI_KEY", "")

# blocca directory / terze parti + news/magazine (falsi positivi frequenti)
BLOCKED_DOMAINS = {
    "linkedin.com",
    "facebook.com",
    "instagram.com",
    "x.com",
    "twitter.com",
    "paginegialle.it",
    "paginebianche.it",
    "kompass.com",
    "europages.com",
    "crunchbase.com",
    "dnb.com",
    "bloomberg.com",
    "reuters.com",
    "wikipedia.org",
    "google.com",
    "maps.google.com",
    "google.it",
    # news / magazine / portali
    "alvolante.it",
    "quattroruote.it",
    "ilsole24ore.com",
    "repubblica.it",
    "corriere.it",
    "ansa.it",
    "it.wikipedia.org",
    "wikipedia.it",
}

app = FastAPI()


class EnrichRequest(BaseModel):
    companies: List[str]
    output_filename: Optional[str] = "domini_compilati.xlsx"


def normalize_domain(d: str) -> str:
    d = d.strip().lower()
    d = d.replace("www.", "")
    if "://" in d:
        d = urlparse(d).netloc.lower().replace("www.", "")
    d = d.split("/")[0]
    d = re.sub(r"[^a-z0-9\.\-]", "", d)
    return d


def to_root_domain(dom: str) -> str:
    dom = normalize_domain(dom)
    parts = dom.split(".")
    if len(parts) <= 2:
        return dom

    two_level_tlds = {"co.uk", "com.au", "co.jp", "com.br", "com.tr"}
    last2 = ".".join(parts[-2:])
    last3 = ".".join(parts[-3:])

    if last2 in two_level_tlds and len(parts) >= 3:
        return last3
    return last2


def is_blocked(domain: str) -> bool:
    dom = normalize_domain(domain)
    for b in BLOCKED_DOMAINS:
        if dom == b or dom.endswith("." + b):
            return True
    return False


def tld_score_bonus(root_domain: str) -> float:
    d = normalize_domain(root_domain)
    if d.endswith(".com"):
        return 0.18
    if d.endswith(".it"):
        return 0.15
    if d.endswith(".org"):
        return 0.05
    if d.endswith(".net"):
        return -0.10
    if d.endswith(".info"):
        return -0.25
    return 0.0


def serpapi_search(company: str, mode: str = "it") -> List[str]:
    """
    Ricerca SerpAPI con retry/backoff per stabilità (429/5xx/timeouts).
    mode:
      - "it": query italiane corporate
      - "en": fallback inglese
    """
    if not SERPAPI_KEY:
        raise RuntimeError("SERPAPI_KEY non configurata")

    if mode == "en":
        queries = [f"\"{company}\" official website", f"\"{company}\" contacts"]
        hl, gl = "en", "us"
    else:
        queries = [f"\"{company}\" sito ufficiale", f"\"{company}\" contatti", f"\"{company}\" partita iva"]
        hl, gl = "it", "it"

    urls: List[str] = []

    for q in queries:
        params = {
            "engine": "google",
            "q": q,
            "hl": hl,
            "gl": gl,
            "num": 10,
            "api_key": SERPAPI_KEY,
        }

        backoffs = [0, 1.5, 3.5]  # secondi
        last_err = None

        for wait_s in backoffs:
            if wait_s:
                time.sleep(wait_s)

            try:
                r = requests.get("https://serpapi.com/search.json", params=params, timeout=25)

                # rate limit / temporanei -> retry
                if r.status_code in (429, 500, 502, 503, 504):
                    last_err = RuntimeError(f"SerpAPI HTTP {r.status_code}")
                    continue

                r.raise_for_status()
                data = r.json()
                for item in (data.get("organic_results") or []):
                    link = item.get("link")
                    if link:
                        urls.append(link)

                last_err = None
                break

            except Exception as e:
                last_err = e
                continue

        if last_err is not None:
            raise last_err

    seen = set()
    urls = [u for u in urls if not (u in seen or seen.add(u))]
    return urls


def fetch_homepage_text(root_domain: str, timeout_s: int = 8) -> Optional[str]:
    try:
        resp = requests.get(f"https://{root_domain}", timeout=timeout_s, headers={"User-Agent": "Mozilla/5.0"})
        return (resp.text or "").lower()
    except Exception:
        try:
            resp = requests.get(f"http://{root_domain}", timeout=timeout_s, headers={"User-Agent": "Mozilla/5.0"})
            return (resp.text or "").lower()
        except Exception:
            return None


def pick_best_domain(company: str) -> Tuple[str, float]:
    start = time.time()
    MAX_SECONDS_PER_COMPANY = 20  # più alto per qualità/stabilità

    def time_left() -> bool:
        return (time.time() - start) <= MAX_SECONDS_PER_COMPANY

    # tokenizzazione
    raw_tokens = [t for t in re.split(r"\W+", company.lower()) if len(t) >= 3]
    stop = {
        "spa", "s", "p", "a", "s.p.a", "s.p.a.", "srl", "s.r.l", "s.r.l.",
        "societa", "società", "company", "group", "holding"
    }
    tokens = [t for t in raw_tokens if t not in stop][:3]
    primary = tokens[0] if tokens else ""

    # hard-fallback utile (STMicroelectronics -> st.com)
    if "stmicroelectronics" in company.lower() or "st microelectronics" in company.lower():
        if time_left():
            st_text = fetch_homepage_text("st.com", timeout_s=6)
            if st_text and ("microelectronics" in st_text or "st.com" in st_text or "st " in st_text):
                return "st.com", 0.95

    def evaluate_urls(urls: List[str]) -> Tuple[str, float]:
        candidates: List[str] = []
        for u in urls:
            host = normalize_domain(u)
            if not host:
                continue
            root = to_root_domain(host)
            if not root or is_blocked(root):
                continue
            candidates.append(root)

        seen = set()
        candidates = [c for c in candidates if not (c in seen or seen.add(c))]

        best = ("NON TROVATO", 0.0)

        for dom_root in candidates[:4]:
            if not time_left():
                break

            text = fetch_homepage_text(dom_root, timeout_s=10)
            if not text:
                continue

            score = 0.0
            score += tld_score_bonus(dom_root)

            # boost token nel dominio
            if primary and primary in dom_root:
                score += 0.6

            # token nella pagina
            token_hits = sum(1 for tok in tokens if tok and tok in text)
            if token_hits >= 2:
                score += 0.6
            elif token_hits == 1:
                score += 0.25

            # segnali corporate
            if ("partita iva" in text) or ("p.iva" in text) or ("codice fiscale" in text) or ("vat" in text):
                score += 0.35
            if ("contatti" in text) or ("chi siamo" in text) or ("about" in text):
                score += 0.1
            if ("cookie" in text) or ("privacy" in text):
                score += 0.05

            # penalità editoriali
            if ("news" in text) or ("newsletter" in text) or ("articolo" in text) or ("abbonati" in text):
                score -= 0.35
            if ("directory" in text) or ("scheda azienda" in text):
                score -= 0.5

            # anti-falso positivo se 1 token e dominio non contiene primary
            if len(tokens) <= 1 and primary and (primary not in dom_root):
                if not (("partita iva" in text) or ("p.iva" in text) or ("vat" in text) or (company.lower() in text)):
                    score -= 0.6

            if score > best[1]:
                best = (dom_root, score)

            if best[1] >= 0.92:
                break

        return best

    # 1) IT
    try:
        urls_it = serpapi_search(company, mode="it")
    except Exception:
        urls_it = []

    best_dom, best_score = evaluate_urls(urls_it)

    # 2) fallback primary.it/.com
    if best_score < 0.78 and primary and 2 <= len(primary) <= 10 and time_left():
        for tld in ("it", "com"):
            if not time_left():
                break
            guess = to_root_domain(f"{primary}.{tld}")
            if is_blocked(guess):
                continue
            text = fetch_homepage_text(guess, timeout_s=9)
            if not text:
                continue
            if ("partita iva" in text) or ("p.iva" in text) or ("vat" in text) or (primary in text):
                return guess, 0.85 + tld_score_bonus(guess)

    # 3) fallback EN se basso
    if (best_dom == "NON TROVATO" or best_score < 0.78) and time_left():
        try:
            urls_en = serpapi_search(company, mode="en")
        except Exception:
            urls_en = []
        dom2, sc2 = evaluate_urls(urls_en)
        if sc2 > best_score:
            best_dom, best_score = dom2, sc2

    if best_score < 0.78:
        return "NON TROVATO", best_score

    return to_root_domain(best_dom), best_score


def require_bearer_token(authorization: str | None) -> None:
    if not APP_API_KEY:
        raise HTTPException(status_code=500, detail="APP_API_KEY non configurata sul server")

    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")

    token = authorization.split(" ", 1)[1].strip()
    if token != APP_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


@app.post("/enrich/domains")
def enrich_domains(req: EnrichRequest, authorization: str | None = Header(default=None)):
    require_bearer_token(authorization)

    wb = Workbook()
    ws = wb.active
    ws.title = "Sheet1"
    ws["A1"] = "Ragione sociale"
    ws["B1"] = "Dominio"

    for i, name in enumerate(req.companies, start=2):
        ws[f"A{i}"] = name  # NON modificare
        domain, _score = pick_best_domain(name)
        ws[f"B{i}"] = domain if domain else "NON TROVATO"

        # micro delay: riduce rate limit e rende più stabile su run lunghi
        time.sleep(0.25)

    buf = io.BytesIO()
    wb.save(buf)
    b64 = base64.b64encode(buf.getvalue()).decode("utf-8")

    return {
        "openaiFileResponse": [
            {
                "name": req.output_filename or "domini_compilati.xlsx",
                "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "content": b64,
            }
        ]
    }


@app.get("/health")
def health():
    return {"ok": True}
