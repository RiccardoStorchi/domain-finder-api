import base64
import io
import os
import re
from typing import List, Optional, Tuple
from urllib.parse import urlparse

import requests
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from openpyxl import Workbook

APP_API_KEY = os.getenv("APP_API_KEY", "")
SERPAPI_KEY = os.getenv("SERPAPI_KEY", "")

# blocca directory / terze parti
BLOCKED_DOMAINS = {
    "linkedin.com", "facebook.com", "instagram.com", "x.com", "twitter.com",
    "paginegialle.it", "paginebianche.it", "kompass.com", "europages.com",
    "crunchbase.com", "dnb.com", "bloomberg.com", "reuters.com", "wikipedia.org",
    "google.com", "maps.google.com", "google.it",
}

app = FastAPI()

class EnrichRequest(BaseModel):
    companies: List[str]
    output_filename: Optional[str] = "domini_compilati.xlsx"

def normalize_domain(d: str) -> str:
    d = d.strip().lower()
    d = d.replace("www.", "")
    # se arriva un URL, estrai host
    if "://" in d:
        d = urlparse(d).netloc.lower().replace("www.", "")
    # togli path se presente
    d = d.split("/")[0]
    # pulizia base
    d = re.sub(r"[^a-z0-9\.\-]", "", d)
    return d

def is_blocked(domain: str) -> bool:
    dom = normalize_domain(domain)
    # blocco diretto e blocco su sottodomini
    for b in BLOCKED_DOMAINS:
        if dom == b or dom.endswith("." + b):
            return True
    return False

def serpapi_search(company: str) -> List[str]:
    """
    Ritorna una lista di URL candidati (ordinati).
    """
    if not SERPAPI_KEY:
        raise RuntimeError("SERPAPI_KEY non configurata")

    params = {
        "engine": "google",
        "q": f"\"{company}\" sito ufficiale",
        "hl": "it",
        "gl": "it",
        "num": 10,
        "api_key": SERPAPI_KEY,
    }
    r = requests.get("https://serpapi.com/search.json", params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    urls = []
    for item in (data.get("organic_results") or []):
        link = item.get("link")
        if link:
            urls.append(link)
    return urls

def pick_best_domain(company: str) -> Tuple[str, float]:
    """
    Strategie prudente:
    - prende domini dai primi risultati
    - scarta directory/terze parti
    - prova a verificare che la home contenga un segnale minimo (name/brand)
    - se non supera soglia: NON TROVATO
    """
    try:
        urls = serpapi_search(company)
    except Exception:
        return "NON TROVATO", 0.0

    candidates = []
    for u in urls:
        host = normalize_domain(u)
        if not host or is_blocked(host):
            continue
        # scarta host chiaramente non corporate (es: .pdf su dominio terzo) -> già coperto dai blocchi
        candidates.append(host)

    # de-dup preservando ordine
    seen = set()
    candidates = [c for c in candidates if not (c in seen or seen.add(c))]

    # verifica leggera: scarica home e cerca occorrenza di una parola "forte"
    # (non perfetto, ma meglio del “primo risultato”)
    tokens = [t for t in re.split(r"\W+", company.lower()) if len(t) >= 4][:3]
    best = ("NON TROVATO", 0.0)

    for dom in candidates[:5]:
        try:
            resp = requests.get(f"https://{dom}", timeout=15, headers={"User-Agent": "Mozilla/5.0"})
            text = (resp.text or "").lower()
        except Exception:
            # prova http se https non va
            try:
                resp = requests.get(f"http://{dom}", timeout=15, headers={"User-Agent": "Mozilla/5.0"})
                text = (resp.text or "").lower()
            except Exception:
                continue

        score = 0.0
        # segnali base
        if any(tok in text for tok in tokens):
            score += 0.7
        if "cookie" in text or "privacy" in text:
            score += 0.1
        if "contatti" in text or "chi siamo" in text or "about" in text:
            score += 0.1

        # penalità: siti troppo “portalosi”
        if "directory" in text or "scheda azienda" in text:
            score -= 0.4

        if score > best[1]:
            best = (dom, score)

        # soglia prudente: accetta solo se abbastanza convinto
        if best[1] >= 0.8:
            break

    if best[1] < 0.75:
        return "NON TROVATO", best[1]
    return best[0], best[1]

def require_api_key(x_api_key: str | None):
    if not APP_API_KEY:
        # se non settata, blocca comunque: meglio sicuro
        raise HTTPException(status_code=500, detail="APP_API_KEY non configurata sul server")
    if not x_api_key or x_api_key != APP_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

@app.post("/enrich/domains")
def enrich_domains(req: EnrichRequest, x_api_key: Optional[str] = Header(default=None)):
    require_api_key(x_api_key)

    wb = Workbook()
    ws = wb.active
    ws.title = "Sheet1"
    ws["A1"] = "Ragione sociale"
    ws["B1"] = "Dominio"

    for i, name in enumerate(req.companies, start=2):
        ws[f"A{i}"] = name  # NON modificare
        domain, _score = pick_best_domain(name)
        ws[f"B{i}"] = domain if domain else "NON TROVATO"

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
