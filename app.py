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

# blocca directory / terze parti + news/magazine (falsi positivi frequenti)
BLOCKED_DOMAINS = {
    "linkedin.com", "facebook.com", "instagram.com", "x.com", "twitter.com",
    "paginegialle.it", "paginebianche.it", "kompass.com", "europages.com",
    "crunchbase.com", "dnb.com", "bloomberg.com", "reuters.com", "wikipedia.org",
    "google.com", "maps.google.com", "google.it",

    # news / magazine / portali (falsi positivi frequenti)
    "alvolante.it", "quattroruote.it", "ilsole24ore.com", "repubblica.it",
    "corriere.it", "ansa.it", "it.wikipedia.org", "wikipedia.it",
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
    """Ritorna una lista di URL candidati (ordinati)."""
    if not SERPAPI_KEY:
        raise RuntimeError("SERPAPI_KEY non configurata")

    # query più "corporate"
    queries = [
        f"\"{company}\" sito ufficiale",
        f"\"{company}\" contatti",
        f"\"{company}\" partita iva",
    ]

    urls: List[str] = []
    for q in queries:
        params = {
            "engine": "google",
            "q": q,
            "hl": "it",
            "gl": "it",
            "num": 10,
            "api_key": SERPAPI_KEY,
        }
        r = requests.get("https://serpapi.com/search.json", params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        for item in (data.get("organic_results") or []):
            link = item.get("link")
            if link:
                urls.append(link)

    # de-dup preservando ordine
    seen = set()
    urls = [u for u in urls if not (u in seen or seen.add(u))]
    return urls


def pick_best_domain(company: str) -> Tuple[str, float]:
    """
    Strategia prudente:
    - prende domini dai risultati
    - scarta directory/terze parti/news
    - scoring più severo se nome generico (1 token)
    - fallback: prova primary.it / primary.com
    """
    try:
        urls = serpapi_search(company)
    except Exception:
        return "NON TROVATO", 0.0

    candidates: List[str] = []
    for u in urls:
        host = normalize_domain(u)
        if not host or is_blocked(host):
            continue
        candidates.append(host)

    # de-dup preservando ordine
    seen = set()
    candidates = [c for c in candidates if not (c in seen or seen.add(c))]

    # tokenizzazione più intelligente
    raw_tokens = [t for t in re.split(r"\W+", company.lower()) if len(t) >= 3]
    stop = {"spa", "s", "p", "a", "s.p.a", "s.p.a.", "srl", "s.r.l", "s.r.l.", "societa", "società",
            "company", "group", "holding"}
    tokens = [t for t in raw_tokens if t not in stop]
    tokens = tokens[:3]
    primary = tokens[0] if tokens else ""

    best = ("NON TROVATO", 0.0)

    for dom in candidates[:6]:
        try:
            resp = requests.get(f"https://{dom}", timeout=15, headers={"User-Agent": "Mozilla/5.0"})
            text = (resp.text or "").lower()
        except Exception:
            try:
                resp = requests.get(f"http://{dom}", timeout=15, headers={"User-Agent": "Mozilla/5.0"})
                text = (resp.text or "").lower()
            except Exception:
                continue

        score = 0.0

        # 1) BOOST: il dominio contiene il token principale (ottimo segnale corporate)
        if primary and primary in dom:
            score += 0.6

        # 2) presenza token nella pagina (prudente)
        token_hits = sum(1 for tok in tokens if tok and tok in text)
        if token_hits >= 2:
            score += 0.6
        elif token_hits == 1:
            score += 0.25  # prudente: evita news/blog

        # 3) segnali corporate tipici
        if ("partita iva" in text) or ("p.iva" in text) or ("codice fiscale" in text) or ("vat" in text):
            score += 0.35
        if ("contatti" in text) or ("chi siamo" in text) or ("about" in text):
            score += 0.1
        if ("cookie" in text) or ("privacy" in text):
            score += 0.05

        # 4) penalità per siti editoriali/portalosi
        if ("news" in text) or ("newsletter" in text) or ("articolo" in text) or ("abbonati" in text):
            score -= 0.35
        if ("directory" in text) or ("scheda azienda" in text):
            score -= 0.5

        # 5) regola anti-falso positivo: se ho solo 1 token e il dominio NON lo contiene,
        # accetto solo con segnali legali (P.IVA/VAT) o ragione sociale completa nel testo
        if len(tokens) <= 1 and primary and (primary not in dom):
            if not (("partita iva" in text) or ("p.iva" in text) or ("vat" in text) or (company.lower() in text)):
                score -= 0.6

        if score > best[1]:
            best = (dom, score)

        # stop early se molto convincente
        if best[1] >= 0.9:
            break

    # Fallback: se score basso e token corto/forte (es. ima), prova primary.it e primary.com
    if best[1] < 0.75 and primary and 2 <= len(primary) <= 8:
        for tld in ("it", "com"):
            guess = f"{primary}.{tld}"
            if is_blocked(guess):
                continue
            try:
                resp = requests.get(f"https://{guess}", timeout=12, headers={"User-Agent": "Mozilla/5.0"})
                text = (resp.text or "").lower()
            except Exception:
                try:
                    resp = requests.get(f"http://{guess}", timeout=12, headers={"User-Agent": "Mozilla/5.0"})
                    text = (resp.text or "").lower()
                except Exception:
                    continue

            if ("partita iva" in text) or ("p.iva" in text) or ("vat" in text) or (primary in text):
                return guess, 0.8

    if best[1] < 0.75:
        return "NON TROVATO", best[1]
    return best[0], best[1]


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
    # Auth: Authorization: Bearer <APP_API_KEY>
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
