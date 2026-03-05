"""
LEEF Datashare Routes - Geextraheerd uit vergunningcheck/geo_api.py

Dit bestand wordt gegenereerd door export_from_vergunningcheck.py.
Handmatige wijzigingen worden overschreven bij de volgende sync.
"""
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
import httpx
import io
import os
import base64
import time
import json as _json
import asyncio
import math
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed

from db_config import get_connection, get_cursor, return_connection
from logging_config import get_logger
from zaaktype_mapping import _ZAAKTYPE_MAPPING

logger = get_logger(__name__)

router = APIRouter(prefix="/geo", tags=["leef-datashare"])

# =============================================================================
# LEEF DATASHARE API - Zaken & Documenten (OAuth2 Client Credentials)
# =============================================================================

_leef_token_cache = {"token": None, "expires_at": 0}


def _get_leef_token() -> str:
    """Haal een OAuth2 access token op via client credentials, met caching."""
    if _leef_token_cache["token"] and time.time() < _leef_token_cache["expires_at"] - 60:
        return _leef_token_cache["token"]

    client_id = os.environ.get("LEEF_CLIENT_ID", "")
    client_secret = os.environ.get("LEEF_CLIENT_SECRET", "")
    token_url = os.environ.get("LEEF_OAUTH_TOKEN_URL", "")

    if not client_id or not client_secret or not token_url:
        raise HTTPException(status_code=500, detail="LEEF OAuth2 credentials niet geconfigureerd")

    credentials = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()

    try:
        with httpx.Client(timeout=15.0) as client:
            response = client.post(
                token_url,
                headers={
                    "Authorization": f"Basic {credentials}",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                data={"grant_type": "client_credentials"},
            )
            response.raise_for_status()
            token_data = response.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"LEEF OAuth2 token request mislukt: {e.response.status_code}")
        raise HTTPException(status_code=502, detail="Kon geen LEEF token ophalen")
    except Exception as e:
        logger.error(f"LEEF OAuth2 token request fout: {e}")
        raise HTTPException(status_code=502, detail="LEEF token endpoint niet bereikbaar")

    _leef_token_cache["token"] = token_data["access_token"]
    _leef_token_cache["expires_at"] = time.time() + token_data.get("expires_in", 3600)
    return _leef_token_cache["token"]


def _leef_api_get(path: str, params: Optional[Dict] = None) -> httpx.Response:
    """Doe een GET request naar de LEEF Datashare API met OAuth2 token."""
    base_url = os.environ.get("LEEF_DATASHARE_URL", "")
    if not base_url:
        raise HTTPException(status_code=500, detail="LEEF_DATASHARE_URL niet geconfigureerd")

    token = _get_leef_token()
    url = f"{base_url}{path}"

    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.get(
                url,
                headers={"Authorization": f"Bearer {token}"},
                params=params,
            )
            response.raise_for_status()
            return response
    except httpx.HTTPStatusError as e:
        logger.error(f"LEEF API fout {e.response.status_code}: {path}")
        raise HTTPException(status_code=e.response.status_code, detail=f"LEEF API fout: {e.response.status_code}")
    except Exception as e:
        logger.error(f"LEEF API request fout: {e}")
        raise HTTPException(status_code=502, detail="LEEF API niet bereikbaar")


def _leef_api_post(path: str, json_body: Optional[Dict] = None) -> httpx.Response:
    """Doe een POST request naar de LEEF Datashare API met OAuth2 token."""
    base_url = os.environ.get("LEEF_DATASHARE_URL", "")
    if not base_url:
        raise HTTPException(status_code=500, detail="LEEF_DATASHARE_URL niet geconfigureerd")

    token = _get_leef_token()
    url = f"{base_url}{path}"

    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.post(
                url,
                headers={"Authorization": f"Bearer {token}"},
                json=json_body,
            )
            response.raise_for_status()
            return response
    except httpx.HTTPStatusError as e:
        logger.error(f"LEEF API POST fout {e.response.status_code}: {path}")
        raise HTTPException(status_code=e.response.status_code, detail=f"LEEF API fout: {e.response.status_code}")
    except Exception as e:
        logger.error(f"LEEF API POST request fout: {e}")
        raise HTTPException(status_code=502, detail="LEEF API niet bereikbaar")


_leef_zaken_cache = {"items": None, "fetched_at": 0}
LEEF_ZAKEN_CACHE_TTL = 300  # 5 minuten cache


def _invalidate_zaken_cache():
    """Invalideer de zaken-cache zodat classificatie-updates direct zichtbaar zijn op het hoofdscherm."""
    _leef_zaken_cache["items"] = None
    _leef_zaken_cache["fetched_at"] = 0


def _enrich_with_classification_stats(items: list):
    """Voeg classificatie-statistieken toe aan zaak-items vanuit de database."""
    if not items:
        return
    zaak_ids = [item.get("id") for item in items if item.get("id")]
    if not zaak_ids:
        return

    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        placeholders = ",".join(["%s"] * len(zaak_ids))
        zaak_id_ints = [int(z) if str(z).isdigit() else 0 for z in zaak_ids]
        cursor.execute(f"""
            WITH doc_status AS (
                SELECT zaak_id, document_id,
                    bool_and(status = 'goedgekeurd') AS all_approved,
                    bool_or(status = 'concept') AS has_concept
                FROM leef_doc_classificatie
                GROUP BY zaak_id, document_id
            )
            SELECT zaak_id,
                COUNT(*) AS classified_docs,
                COUNT(*) FILTER (WHERE all_approved) AS approved_docs,
                COUNT(*) FILTER (WHERE has_concept) AS concept_docs
            FROM doc_status WHERE zaak_id IN ({placeholders})
            GROUP BY zaak_id
        """, zaak_id_ints)

        stats_map = {}
        for row in cursor.fetchall():
            stats_map[row[0]] = {
                "classified": row[1],
                "approved": row[2],
                "concept": row[3],
            }

        for item in items:
            zaak_id = int(item["id"]) if str(item.get("id", "")).isdigit() else 0
            stats = stats_map.get(zaak_id, {"classified": 0, "approved": 0, "concept": 0})
            item["_classified_count"] = stats["classified"]
            item["_approved_count"] = stats["approved"]
            item["_concept_count"] = stats["concept"]
    except Exception as e:
        logger.warning(f"Classificatie stats ophalen mislukt: {e}")
        for item in items:
            item.setdefault("_classified_count", 0)
            item.setdefault("_approved_count", 0)
            item.setdefault("_concept_count", 0)
    finally:
        return_connection(conn)


def _fetch_leef_zaken() -> list:
    """Haal zaken op uit de LEEF API met document counts en classificatie stats. Cached voor 5 min."""
    if _leef_zaken_cache["items"] is not None and time.time() < _leef_zaken_cache["fetched_at"] + LEEF_ZAKEN_CACHE_TTL:
        return _leef_zaken_cache["items"]

    response = _leef_api_get("/zaken")
    data = response.json()
    items = data.get("items", []) if isinstance(data, dict) else data

    _enrich_with_doc_counts(items)
    _enrich_with_classification_stats(items)

    logger.info(f"LEEF Datashare: {len(items)} zaken opgehaald en verrijkt met doc counts + classificatie stats")
    _leef_zaken_cache["items"] = items
    _leef_zaken_cache["fetched_at"] = time.time()
    return items


def _enrich_with_doc_counts(items: list):
    """Haal document counts op voor alle zaken via parallel detail requests."""
    base_url = os.environ.get("LEEF_DATASHARE_URL", "")
    if not base_url or not items:
        return

    token = _get_leef_token()

    def fetch_doc_count(item):
        zaak_id = item.get("id")
        if not zaak_id:
            item["_doc_count"] = 0
            return
        try:
            with httpx.Client(timeout=15.0) as client:
                resp = client.get(
                    f"{base_url}/zaken/{zaak_id}",
                    headers={"Authorization": f"Bearer {token}"},
                )
                resp.raise_for_status()
                detail = resp.json()
                if isinstance(detail, dict) and "items" in detail and detail["items"]:
                    detail = detail["items"][0]
                docs = detail.get("documenten", [])
                item["_doc_count"] = len(docs) if isinstance(docs, list) else 0
                item["documenten"] = docs if isinstance(docs, list) else []
        except Exception as e:
            logger.warning(f"Doc count ophalen mislukt voor zaak {zaak_id}: {e}")
            item["_doc_count"] = 0
            item["documenten"] = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_doc_count, item) for item in items]
        for f in as_completed(futures):
            pass


@router.get("/leef-datashare/zaken")
async def leef_datashare_zaken(
    zoek: Optional[str] = Query(None, description="Zoek op intern_kenmerk of zaak_omschrijving"),
    zaaktype: Optional[str] = Query(None, description="Filter op zaaktype"),
    hoofd_of_sub: Optional[str] = Query(None, description="Filter op hoofd_of_sub"),
):
    """Haal zaken op uit de LEEF Datashare API met filtering."""
    all_items = _fetch_leef_zaken()
    items = list(all_items)

    if zoek:
        zoek_lower = zoek.lower()
        items = [
            z for z in items
            if zoek_lower in str(z.get("intern_kenmerk", "")).lower()
            or zoek_lower in str(z.get("zaak_omschrijving", "")).lower()
            or zoek_lower in str(z.get("zaaktype", "")).lower()
            or zoek_lower in str(z.get("product", "")).lower()
        ]

    if zaaktype:
        items = [z for z in items if z.get("zaaktype") == zaaktype]

    if hoofd_of_sub:
        items = [z for z in items if z.get("hoofd_of_sub") == hoofd_of_sub]

    return {
        "items": items,
        "total": len(items),
        "total_unfiltered": len(all_items),
    }


@router.get("/leef-datashare/zaken/{zaak_id}")
async def leef_datashare_zaak_detail(zaak_id: str):
    """Haal zaak detail op inclusief documenten en subzaken."""
    response = _leef_api_get(f"/zaken/{zaak_id}")
    data = response.json()

    if isinstance(data, dict) and "items" in data and data["items"]:
        return data["items"][0]

    return data


@router.get("/leef-datashare/documenten/{doc_id}")
async def leef_datashare_document(
    doc_id: str,
    inline: bool = Query(False, description="True = inline weergeven (viewer), False = download"),
):
    """Download of bekijk een document uit de LEEF Datashare API (streaming proxy)."""
    base_url = os.environ.get("LEEF_DATASHARE_URL", "")
    if not base_url:
        raise HTTPException(status_code=500, detail="LEEF_DATASHARE_URL niet geconfigureerd")

    token = _get_leef_token()
    url = f"{base_url}/documenten/{doc_id}"

    try:
        client = httpx.Client(timeout=httpx.Timeout(300.0, connect=30.0))
        response = client.send(
            client.build_request("GET", url, headers={"Authorization": f"Bearer {token}"}),
            stream=True,
            follow_redirects=True,
        )
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        logger.error(f"LEEF document download fout {e.response.status_code}: {doc_id}")
        raise HTTPException(status_code=e.response.status_code, detail="Document niet gevonden")
    except Exception as e:
        logger.error(f"LEEF document download fout: {e}")
        raise HTTPException(status_code=502, detail="Kon document niet ophalen")

    content_type = response.headers.get("content-type", "application/octet-stream")
    content_length = response.headers.get("content-length")

    headers = {}
    if content_length:
        headers["Content-Length"] = content_length
    headers["Cache-Control"] = "private, max-age=300"
    headers["X-Frame-Options"] = "SAMEORIGIN"
    headers["Content-Security-Policy"] = "frame-ancestors 'self'"

    if inline:
        headers["Content-Disposition"] = "inline"
    else:
        content_disposition = response.headers.get("content-disposition", "")
        headers["Content-Disposition"] = content_disposition if content_disposition else "attachment"

    def stream_with_cleanup():
        try:
            yield from response.iter_bytes(chunk_size=65536)
        finally:
            response.close()
            client.close()

    return StreamingResponse(
        stream_with_cleanup(),
        media_type=content_type,
        headers=headers,
    )


# =============================================================================
# LEEF DATASHARE - Document Classificatie (LLM)
# =============================================================================

_prompt_cache: Dict[str, Any] = {}
_PROMPT_CACHE_TTL = 300

_DEFAULT_PROMPTS: Dict[str, str] = {
    "vision_prompt": (
        "Beschrijf wat je ziet in deze afbeelding. Is het een bouwtekening, situatieschets, "
        "plattegrond, doorsnede, gevelaanzicht, foto, formulier, brief of iets anders? "
        "Als het een technische tekening is, beschrijf dan de afmetingen, schaal, "
        "tekeningnummer en andere details die je kunt lezen. Antwoord in het Nederlands."
    ),
    "classificatie_prompt": """Classificeer dit document in de onderstaande categorieen en geef een korte samenvatting, omschrijving en toelichting.
Een document kan meerdere classificaties hebben (bijv. een bouwtekening die zowel plattegrond als nieuwe situatie toont).

Documentnaam: {doc_naam}
Eerste tekst uit document:
{tekst_preview}
{image_section}{few_shot_section}
Beschikbare categorieen:
{codes_met_omschrijving}

Antwoord in exact dit JSON formaat (geen andere tekst):
{{"codes": ["code_1", "code_2"], "samenvatting": "Korte samenvatting van de inhoud in 1-3 zinnen", "omschrijving": "Beknopte omschrijving van wat dit document is en bevat", "toelichting": "Toelichting waarom deze classificatie(s) zijn gekozen, met verwijzing naar specifieke kenmerken in het document"}}
Geef minimaal 1 en maximaal 3 codes, op volgorde van relevantie.
Let op: "niet_relevant" en "vervallen" mogen NIET gecombineerd worden met andere codes. Gebruik deze alleen als het document echt niet relevant of vervallen is.""",
    "system_message": "Je bent een document classificatie-assistent voor een Nederlandse gemeente. Antwoord altijd in het gevraagde JSON formaat.",
}


def _get_prompt(key: str) -> str:
    """Haal een prompt op uit de database met 5 min cache, fallback naar default."""
    now = time.time()
    cached = _prompt_cache.get(key)
    if cached and (now - cached["ts"]) < _PROMPT_CACHE_TTL:
        return cached["value"]

    try:
        conn = get_connection()
        cursor = get_cursor(conn)
        try:
            cursor.execute("SELECT value FROM classificatie_prompts WHERE key = %s", (key,))
            row = cursor.fetchone()
            if row:
                _prompt_cache[key] = {"value": row[0], "ts": now}
                return row[0]
        finally:
            return_connection(conn)
    except Exception as e:
        logger.warning(f"Prompt laden uit DB mislukt voor '{key}': {e}")

    default = _DEFAULT_PROMPTS.get(key, "")
    _prompt_cache[key] = {"value": default, "ts": now}
    return default


def _extract_pdf_text(doc_bytes: bytes, max_chars: int = 2000) -> str:
    """Extract tekst uit PDF bytes via pdfplumber."""
    try:
        import pdfplumber
        text_parts = []
        total = 0
        with pdfplumber.open(io.BytesIO(doc_bytes)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text() or ""
                if not page_text.strip():
                    continue
                remaining = max_chars - total
                if remaining <= 0:
                    break
                text_parts.append(page_text[:remaining])
                total += len(page_text[:remaining])
        return "\n".join(text_parts).strip()
    except Exception as e:
        logger.warning(f"PDF tekst extractie mislukt: {e}")
        return ""


def _render_pdf_pages_b64(doc_bytes: bytes, max_pages: int = 2) -> list:
    """Render eerste pagina('s) van PDF naar base64-encoded PNG afbeeldingen."""
    try:
        import pdfplumber
        images_b64 = []
        with pdfplumber.open(io.BytesIO(doc_bytes)) as pdf:
            for i, page in enumerate(pdf.pages[:max_pages]):
                img = page.to_image(resolution=150).original
                buf = io.BytesIO()
                img.save(buf, format="PNG")
                images_b64.append(base64.b64encode(buf.getvalue()).decode("utf-8"))
        return images_b64
    except Exception as e:
        logger.warning(f"PDF pagina rendering mislukt: {e}")
        return []


def _analyze_pdf_images(doc_bytes: bytes, max_pages: int = 2) -> str:
    """Render eerste pagina('s) van PDF naar afbeelding en analyseer via Azure OpenAI of Ollama vision model."""
    images_b64 = _render_pdf_pages_b64(doc_bytes, max_pages)
    if not images_b64:
        return ""

    vision_prompt = _get_prompt("vision_prompt")

    # Probeer Azure OpenAI (vision)
    azure_endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT", "")
    azure_api_key = os.environ.get("AZURE_OPENAI_API_KEY", "")
    vision_deployment = os.environ.get("AZURE_OPENAI_VISION_DEPLOYMENT", os.environ.get("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4o-mini"))
    azure_api_version = os.environ.get("AZURE_OPENAI_API_VERSION", "2024-12-01-preview")

    if azure_endpoint and azure_api_key:
        try:
            content_parts = [{"type": "text", "text": vision_prompt}]
            for img_b64 in images_b64:
                content_parts.append({
                    "type": "image_url",
                    "image_url": {"url": f"data:image/png;base64,{img_b64}", "detail": "high"},
                })

            url = f"{azure_endpoint.rstrip('/')}/openai/deployments/{vision_deployment}/chat/completions?api-version={azure_api_version}"
            with httpx.Client(timeout=120.0) as client:
                resp = client.post(
                    url,
                    headers={"api-key": azure_api_key, "Content-Type": "application/json"},
                    json={
                        "messages": [{"role": "user", "content": content_parts}],
                        "max_tokens": 800,
                        "temperature": 0.2,
                    },
                )
                resp.raise_for_status()
                result = resp.json()
                answer = result["choices"][0]["message"]["content"].strip()
                logger.info(f"Azure OpenAI vision analyse voltooid ({len(answer)} chars)")
                return answer
        except Exception as e:
            logger.warning(f"Azure OpenAI vision analyse mislukt: {e}")

    # Fallback: Ollama vision
    ollama_url = os.environ.get("OLLAMA_URL", "")
    vision_model = os.environ.get("OLLAMA_VISION_MODEL", "")

    if ollama_url and vision_model:
        try:
            with httpx.Client(timeout=120.0) as client:
                resp = client.post(
                    f"{ollama_url}/api/chat",
                    json={
                        "model": vision_model,
                        "messages": [{"role": "user", "content": vision_prompt, "images": images_b64}],
                        "stream": False,
                    },
                )
                resp.raise_for_status()
                result = resp.json()
                return result.get("message", {}).get("content", "").strip()
        except Exception as e:
            logger.warning(f"Ollama vision analyse mislukt: {e}")

    return ""


def _generate_embedding(text: str) -> list:
    """Genereer een embedding vector via Ollama /api/embed."""
    ollama_url = os.environ.get("OLLAMA_URL", "")
    ollama_model = os.environ.get("OLLAMA_MODEL", "qwen2:0.5b")
    if not ollama_url or not text:
        return []
    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.post(
                f"{ollama_url}/api/embed",
                json={"model": ollama_model, "input": text[:1000]},
            )
            resp.raise_for_status()
            data = resp.json()
            embeddings = data.get("embeddings") or data.get("embedding")
            if isinstance(embeddings, list) and len(embeddings) > 0:
                if isinstance(embeddings[0], list):
                    return embeddings[0]
                return embeddings
            return []
    except Exception as e:
        logger.warning(f"Embedding generatie mislukt: {e}")
        return []


def _cosine_similarity(vec1: list, vec2: list) -> float:
    """Bereken cosine similarity tussen twee vectoren."""
    if not vec1 or not vec2 or len(vec1) != len(vec2):
        return 0.0
    dot = sum(a * b for a, b in zip(vec1, vec2))
    norm1 = math.sqrt(sum(a * a for a in vec1))
    norm2 = math.sqrt(sum(b * b for b in vec2))
    if norm1 == 0 or norm2 == 0:
        return 0.0
    return dot / (norm1 * norm2)


def _find_similar_documents(text: str, limit: int = 3) -> list:
    """Vind vergelijkbare eerder goedgekeurde documenten op basis van embeddings."""
    if not text:
        return []
    query_embedding = _generate_embedding(text)
    if not query_embedding:
        return []

    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        cursor.execute(
            """SELECT classificatie_code, embedding_vector, extracted_text_preview
               FROM doc_classificatie_embeddings
               ORDER BY created_at DESC LIMIT 100"""
        )
        rows = cursor.fetchall()
    finally:
        return_connection(conn)

    if not rows:
        return []

    results = []
    for code, vector_json, preview in rows:
        try:
            stored_vec = _json.loads(vector_json)
            sim = _cosine_similarity(query_embedding, stored_vec)
            results.append({"code": code, "text_preview": preview or "", "similarity": sim})
        except Exception:
            continue

    results.sort(key=lambda x: x["similarity"], reverse=True)
    return results[:limit]


def _build_classify_prompt(doc_naam: str, extracted_text: str, classificaties: list, image_description: str = "") -> str:
    """Bouw de classificatie-prompt op."""
    lines = []
    for c in classificaties:
        line = f"- {c['code']}: {c['label']} ({c['omschrijving']})"
        if c.get('herkenning_tips'):
            line += f" [Tips: {c['herkenning_tips']}]"
        lines.append(line)
    codes_met_omschrijving = "\n".join(lines)

    tekst_preview = extracted_text[:1500] if extracted_text else "(geen tekst beschikbaar)"

    image_section = ""
    if image_description:
        image_section = f"\nVisuele beschrijving van het document (via beeldanalyse):\n{image_description}\n"

    few_shot_section = ""
    similar_docs = _find_similar_documents(f"{doc_naam} {(extracted_text or '')[:500]}")
    if similar_docs:
        examples = "\n".join(
            f"  - \"{ex['text_preview'][:100]}...\" -> {ex['code']} (similarity: {ex['similarity']:.2f})"
            for ex in similar_docs if ex["similarity"] > 0.5
        )
        if examples:
            few_shot_section = f"\nEerder goedgekeurde classificaties (voorbeelden):\n{examples}\n"

    template = _get_prompt("classificatie_prompt")
    return template.format(
        doc_naam=doc_naam,
        tekst_preview=tekst_preview,
        image_section=image_section,
        few_shot_section=few_shot_section,
        codes_met_omschrijving=codes_met_omschrijving,
    )


def _parse_classify_response(llm_response: str, classificaties: list) -> dict:
    """Parse het LLM antwoord (JSON of plain text) en match met bekende codes."""
    known_codes = {c["code"] for c in classificaties}
    matched_codes = []
    confidence = 0.0
    samenvatting = ""
    omschrijving = ""
    toelichting = ""

    try:
        clean = llm_response.strip()
        if clean.startswith("```"):
            clean = clean.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
        parsed = _json.loads(clean)
        if isinstance(parsed, dict):
            samenvatting = (parsed.get("samenvatting") or "").strip()
            omschrijving = (parsed.get("omschrijving") or "").strip()
            toelichting = (parsed.get("toelichting") or "").strip()

            codes_val = parsed.get("codes")
            if isinstance(codes_val, list):
                for c in codes_val:
                    cv = str(c).lower().strip()
                    if cv in known_codes and cv not in matched_codes:
                        matched_codes.append(cv)
                if matched_codes:
                    confidence = 0.95

            if not matched_codes:
                code_val = (parsed.get("code") or "").lower().strip()
                if code_val in known_codes:
                    matched_codes = [code_val]
                    confidence = 0.95
    except (_json.JSONDecodeError, ValueError):
        pass

    if not matched_codes:
        response_lower = llm_response.lower().strip().strip("'\"")
        if response_lower in known_codes:
            matched_codes = [response_lower]
            confidence = 0.9
        else:
            for code in known_codes:
                if code in response_lower:
                    matched_codes = [code]
                    confidence = 0.7
                    break

    if not matched_codes:
        matched_codes = ["bijlage"]
        confidence = 0.3

    exclusive_codes = {"niet_relevant", "vervallen"}
    has_exclusive = any(c in exclusive_codes for c in matched_codes)
    has_regular = any(c not in exclusive_codes for c in matched_codes)
    if has_exclusive and has_regular:
        matched_codes = [c for c in matched_codes if c not in exclusive_codes]

    matched_codes = matched_codes[:3]

    return {
        "codes": matched_codes,
        "code": matched_codes[0],
        "confidence": confidence,
        "llm_response": llm_response,
        "samenvatting": samenvatting,
        "omschrijving": omschrijving,
        "toelichting": toelichting,
    }


def _classify_document(doc_naam: str, extracted_text: str, classificaties: list, image_description: str = "") -> dict:
    """Classificeer een document via Azure OpenAI (primair) of Ollama (fallback)."""
    prompt = _build_classify_prompt(doc_naam, extracted_text, classificaties, image_description)

    azure_endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT", "")
    azure_api_key = os.environ.get("AZURE_OPENAI_API_KEY", "")
    classify_deployment = os.environ.get("AZURE_OPENAI_CLASSIFY_DEPLOYMENT", os.environ.get("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4o-mini"))
    azure_api_version = os.environ.get("AZURE_OPENAI_API_VERSION", "2024-12-01-preview")

    if azure_endpoint and azure_api_key:
        try:
            url = f"{azure_endpoint.rstrip('/')}/openai/deployments/{classify_deployment}/chat/completions?api-version={azure_api_version}"
            with httpx.Client(timeout=30.0) as client:
                resp = client.post(
                    url,
                    headers={"api-key": azure_api_key, "Content-Type": "application/json"},
                    json={
                        "messages": [
                            {"role": "system", "content": _get_prompt("system_message")},
                            {"role": "user", "content": prompt},
                        ],
                        "max_tokens": 300,
                        "temperature": 0.1,
                    },
                )
                resp.raise_for_status()
                result = resp.json()
                llm_response = result["choices"][0]["message"]["content"].strip()
                logger.info(f"Azure OpenAI classificatie: {llm_response}")
                return _parse_classify_response(llm_response, classificaties)
        except Exception as e:
            logger.warning(f"Azure OpenAI classificatie mislukt, probeer Ollama fallback: {e}")

    ollama_url = os.environ.get("OLLAMA_URL", "")
    ollama_model = os.environ.get("OLLAMA_MODEL", "qwen2:0.5b")

    if not ollama_url:
        return {"code": None, "confidence": 0.0, "llm_response": "Geen LLM geconfigureerd (Azure OpenAI of Ollama)"}

    try:
        with httpx.Client(timeout=60.0) as client:
            resp = client.post(
                f"{ollama_url}/api/generate",
                json={"model": ollama_model, "prompt": prompt, "stream": False},
            )
            resp.raise_for_status()
            result = resp.json()
    except Exception as e:
        logger.error(f"Ollama classify fout: {e}")
        return {"code": None, "confidence": 0.0, "llm_response": f"LLM fout: {e}"}

    llm_response = result.get("response", "").strip()
    return _parse_classify_response(llm_response, classificaties)


def _classify_single_document_sync(doc: dict, base_url: str, token: str, classificaties: list) -> dict:
    """Sync helper: download, tekst-extractie, vision en classificatie voor 1 document."""
    doc_id = doc.get("id")
    doc_naam = doc.get("naam") or doc.get("name") or "onbekend"
    mime_type = doc.get("mime_type", "")
    is_pdf = "pdf" in mime_type.lower() or doc_naam.lower().endswith(".pdf")

    extracted_text = ""
    doc_bytes = b""
    try:
        with httpx.Client(timeout=60.0) as client:
            doc_resp = client.get(
                f"{base_url}/documenten/{doc_id}",
                headers={"Authorization": f"Bearer {token}"},
                follow_redirects=True,
            )
            doc_resp.raise_for_status()
            doc_bytes = doc_resp.content
            if is_pdf:
                extracted_text = _extract_pdf_text(doc_bytes)
    except Exception as e:
        logger.warning(f"Document download/extract fout voor {doc_id}: {e}")

    image_description = ""
    if is_pdf and doc_bytes:
        image_description = _analyze_pdf_images(doc_bytes)

    result = _classify_document(doc_naam, extracted_text, classificaties, image_description=image_description)
    result["_extracted_text"] = extracted_text
    result["_image_description"] = image_description
    result["_doc_naam"] = doc_naam
    return result


@router.get("/leef-datashare/classificaties")
async def leef_datashare_classificaties():
    """Lijst alle actieve classificatie-categorieen."""
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        try:
            cursor.execute("ALTER TABLE doc_classificaties ADD COLUMN IF NOT EXISTS herkenning_tips TEXT")
            conn.commit()
        except Exception:
            conn.rollback()

        cursor.execute(
            "SELECT code, label, omschrijving, volgorde, herkenning_tips FROM doc_classificaties WHERE actief = true ORDER BY volgorde"
        )
        rows = cursor.fetchall()
        return {
            "items": [
                {"code": r[0], "label": r[1], "omschrijving": r[2], "volgorde": r[3], "herkenning_tips": r[4]}
                for r in rows
            ]
        }
    finally:
        return_connection(conn)


class ClassificatieInput(BaseModel):
    code: str
    label: str
    omschrijving: Optional[str] = None
    actief: bool = True
    volgorde: int = 0
    herkenning_tips: Optional[str] = None


@router.post("/leef-datashare/classificaties")
async def leef_datashare_classificatie_upsert(data: ClassificatieInput):
    """Voeg classificatie-categorie toe of wijzig bestaande."""
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        cursor.execute(
            """INSERT INTO doc_classificaties (code, label, omschrijving, actief, volgorde, herkenning_tips)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (code) DO UPDATE SET
                   label = EXCLUDED.label,
                   omschrijving = EXCLUDED.omschrijving,
                   actief = EXCLUDED.actief,
                   volgorde = EXCLUDED.volgorde,
                   herkenning_tips = EXCLUDED.herkenning_tips""",
            (data.code, data.label, data.omschrijving, data.actief, data.volgorde, data.herkenning_tips),
        )
        conn.commit()
        return {"status": "ok", "code": data.code}
    finally:
        return_connection(conn)


class PromptInput(BaseModel):
    key: str
    value: str


@router.get("/leef-datashare/prompts")
async def leef_datashare_prompts():
    """Lijst alle classificatie-prompts."""
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        cursor.execute("SELECT key, value, updated_at FROM classificatie_prompts ORDER BY key")
        rows = cursor.fetchall()
        return {
            "items": [
                {"key": r[0], "value": r[1], "updated_at": str(r[2]) if r[2] else None}
                for r in rows
            ]
        }
    finally:
        return_connection(conn)


@router.post("/leef-datashare/prompts")
async def leef_datashare_prompt_update(data: PromptInput):
    """Wijzig een classificatie-prompt."""
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        cursor.execute(
            """INSERT INTO classificatie_prompts (key, value, updated_at)
               VALUES (%s, %s, CURRENT_TIMESTAMP)
               ON CONFLICT (key) DO UPDATE SET
                   value = EXCLUDED.value,
                   updated_at = CURRENT_TIMESTAMP""",
            (data.key, data.value),
        )
        conn.commit()
        _prompt_cache.pop(data.key, None)
        return {"status": "ok", "key": data.key}
    finally:
        return_connection(conn)


@router.post("/leef-datashare/zaken/{zaak_id}/classificeer")
async def leef_datashare_classificeer(zaak_id: str):
    """Classificeer alle documenten van een zaak via LLM."""
    response = await asyncio.to_thread(_leef_api_get, f"/zaken/{zaak_id}")
    data = response.json()
    if isinstance(data, dict) and "items" in data and data["items"]:
        zaak = data["items"][0]
    else:
        zaak = data

    documenten = zaak.get("documenten", [])
    if not documenten:
        return {"zaak_id": zaak_id, "resultaten": [], "message": "Geen documenten gevonden"}

    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        cursor.execute(
            "SELECT code, label, omschrijving, herkenning_tips FROM doc_classificaties WHERE actief = true ORDER BY volgorde"
        )
        classificaties = [{"code": r[0], "label": r[1], "omschrijving": r[2], "herkenning_tips": r[3]} for r in cursor.fetchall()]
    finally:
        return_connection(conn)

    if not classificaties:
        raise HTTPException(status_code=500, detail="Geen classificatie-categorieen geconfigureerd")

    conn2 = get_connection()
    cursor2 = get_cursor(conn2)
    try:
        zaak_id_int = int(zaak_id) if zaak_id.isdigit() else 0
        cursor2.execute(
            "SELECT DISTINCT document_id FROM leef_doc_classificatie WHERE zaak_id = %s",
            (zaak_id_int,),
        )
        already_classified = {r[0] for r in cursor2.fetchall()}
    finally:
        return_connection(conn2)

    base_url = os.environ.get("LEEF_DATASHARE_URL", "")
    token = _get_leef_token()
    resultaten = []
    skipped = 0

    for doc in documenten:
        doc_id = doc.get("id")
        doc_naam = doc.get("naam") or doc.get("name") or "onbekend"

        doc_id_int = int(doc_id) if str(doc_id).isdigit() else 0
        if doc_id_int in already_classified:
            skipped += 1
            continue

        result = await asyncio.to_thread(
            _classify_single_document_sync, doc, base_url, token, classificaties
        )
        extracted_text = result.pop("_extracted_text", "")
        image_description = result.pop("_image_description", "")
        doc_naam = result.pop("_doc_naam", doc_naam)

        conn = get_connection()
        cursor = get_cursor(conn)
        try:
            zaak_id_int = int(zaak_id) if zaak_id.isdigit() else 0
            doc_id_int = int(doc_id) if str(doc_id).isdigit() else 0

            cursor.execute(
                "DELETE FROM leef_doc_classificatie WHERE zaak_id = %s AND document_id = %s",
                (zaak_id_int, doc_id_int),
            )

            for code in result["codes"]:
                cursor.execute(
                    """INSERT INTO leef_doc_classificatie
                       (zaak_id, document_id, document_naam, classificatie_code, confidence, llm_response, extracted_text, status, image_description, samenvatting, doc_omschrijving, doc_toelichting)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, 'concept', %s, %s, %s, %s)
                       ON CONFLICT (zaak_id, document_id, classificatie_code) DO UPDATE SET
                           confidence = EXCLUDED.confidence,
                           llm_response = EXCLUDED.llm_response,
                           extracted_text = EXCLUDED.extracted_text,
                           status = 'concept',
                           image_description = EXCLUDED.image_description,
                           samenvatting = EXCLUDED.samenvatting,
                           doc_omschrijving = EXCLUDED.doc_omschrijving,
                           doc_toelichting = EXCLUDED.doc_toelichting,
                           classified_at = CURRENT_TIMESTAMP""",
                    (
                        zaak_id_int, doc_id_int, doc_naam, code,
                        result["confidence"], result["llm_response"],
                        extracted_text[:500] if extracted_text else None,
                        image_description[:1000] if image_description else None,
                        result.get("samenvatting", "")[:1000] or None,
                        result.get("omschrijving", "")[:1000] or None,
                        result.get("toelichting", "")[:2000] or None,
                    ),
                )
            conn.commit()
        finally:
            return_connection(conn)

        labels = [next((c["label"] for c in classificaties if c["code"] == code), code) for code in result["codes"]]

        resultaten.append({
            "document_id": doc_id,
            "document_naam": doc_naam,
            "classificatie_codes": result["codes"],
            "classificatie_labels": labels,
            "classificatie_code": result["code"],
            "classificatie_label": labels[0] if labels else result["code"],
            "confidence": result["confidence"],
            "omschrijving": result.get("omschrijving") or None,
            "toelichting": result.get("toelichting") or None,
            "status": "concept",
        })

    _invalidate_zaken_cache()
    return {"zaak_id": zaak_id, "resultaten": resultaten, "overgeslagen": skipped, "totaal_documenten": len(documenten)}


@router.post("/leef-datashare/zaken/{zaak_id}/classificeer/{doc_id}")
async def leef_datashare_classificeer_enkel(zaak_id: str, doc_id: int):
    """Classificeer een enkel document van een zaak via LLM."""
    try:
        response = await asyncio.to_thread(_leef_api_get, f"/zaken/{zaak_id}")
        data = response.json()
        if isinstance(data, dict) and "items" in data and data["items"]:
            zaak = data["items"][0]
        else:
            zaak = data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"LEEF API fout: {str(e)}")

    documenten = zaak.get("documenten", [])
    doc = next((d for d in documenten if d.get("id") == doc_id or str(d.get("id")) == str(doc_id)), None)
    if not doc:
        raise HTTPException(status_code=404, detail=f"Document {doc_id} niet gevonden in zaak {zaak_id}")

    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        cursor.execute(
            "SELECT code, label, omschrijving, herkenning_tips FROM doc_classificaties WHERE actief = true ORDER BY volgorde"
        )
        classificaties = [{"code": r[0], "label": r[1], "omschrijving": r[2], "herkenning_tips": r[3]} for r in cursor.fetchall()]
    finally:
        return_connection(conn)

    if not classificaties:
        raise HTTPException(status_code=500, detail="Geen classificatie-categorieen geconfigureerd")

    base_url = os.environ.get("LEEF_DATASHARE_URL", "")
    token = _get_leef_token()

    result = await asyncio.to_thread(
        _classify_single_document_sync, doc, base_url, token, classificaties
    )
    extracted_text = result.pop("_extracted_text", "")
    image_description = result.pop("_image_description", "")
    doc_naam = result.pop("_doc_naam", doc.get("naam") or doc.get("name") or "onbekend")

    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        zaak_id_int = int(zaak_id) if zaak_id.isdigit() else 0

        cursor.execute(
            "DELETE FROM leef_doc_classificatie WHERE zaak_id = %s AND document_id = %s",
            (zaak_id_int, doc_id),
        )

        for code in result["codes"]:
            cursor.execute(
                """INSERT INTO leef_doc_classificatie
                   (zaak_id, document_id, document_naam, classificatie_code, confidence, llm_response, extracted_text, status, image_description, samenvatting, doc_omschrijving, doc_toelichting)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, 'concept', %s, %s, %s, %s)
                   ON CONFLICT (zaak_id, document_id, classificatie_code) DO UPDATE SET
                       confidence = EXCLUDED.confidence,
                       llm_response = EXCLUDED.llm_response,
                       extracted_text = EXCLUDED.extracted_text,
                       status = 'concept',
                       image_description = EXCLUDED.image_description,
                       samenvatting = EXCLUDED.samenvatting,
                       doc_omschrijving = EXCLUDED.doc_omschrijving,
                       doc_toelichting = EXCLUDED.doc_toelichting,
                       classified_at = CURRENT_TIMESTAMP""",
                (
                    zaak_id_int, doc_id, doc_naam, code,
                    result["confidence"], result["llm_response"],
                    extracted_text[:500] if extracted_text else None,
                    image_description[:1000] if image_description else None,
                    result.get("samenvatting", "")[:1000] or None,
                    result.get("omschrijving", "")[:1000] or None,
                    result.get("toelichting", "")[:2000] or None,
                ),
            )
        conn.commit()
    finally:
        return_connection(conn)

    labels = [next((c["label"] for c in classificaties if c["code"] == code), code) for code in result["codes"]]

    _invalidate_zaken_cache()
    return {
        "zaak_id": zaak_id,
        "document_id": doc_id,
        "document_naam": doc_naam,
        "classificatie_codes": result["codes"],
        "classificatie_labels": labels,
        "classificatie_code": result["code"],
        "classificatie_label": labels[0] if labels else result["code"],
        "confidence": result["confidence"],
        "llm_response": result["llm_response"],
        "samenvatting": result.get("samenvatting") or None,
        "omschrijving": result.get("omschrijving") or None,
        "toelichting": result.get("toelichting") or None,
        "image_description": image_description or None,
        "status": "concept",
    }


@router.get("/leef-datashare/zaken/{zaak_id}/classificaties")
async def leef_datashare_zaak_classificaties(zaak_id: str):
    """Haal opgeslagen classificaties op voor een zaak, gegroepeerd per document."""
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        cursor.execute(
            """SELECT lc.document_id, lc.document_naam, lc.classificatie_code,
                      dc.label AS classificatie_label, lc.confidence, lc.classified_at,
                      lc.status, lc.image_description, lc.doc_omschrijving, lc.doc_toelichting
               FROM leef_doc_classificatie lc
               LEFT JOIN doc_classificaties dc ON dc.code = lc.classificatie_code
               WHERE lc.zaak_id = %s
               ORDER BY lc.document_id, lc.confidence DESC""",
            (int(zaak_id) if zaak_id.isdigit() else 0,),
        )
        rows = cursor.fetchall()

        docs_map = OrderedDict()
        for r in rows:
            doc_id = r[0]
            label_entry = {
                "classificatie_code": r[2],
                "classificatie_label": r[3] or r[2],
                "confidence": r[4],
                "classified_at": str(r[5]) if r[5] else None,
                "status": r[6] or "concept",
                "image_description": r[7],
                "omschrijving": r[8],
                "toelichting": r[9],
            }
            if doc_id not in docs_map:
                docs_map[doc_id] = {
                    "document_id": doc_id,
                    "document_naam": r[1],
                    "labels": [],
                }
            docs_map[doc_id]["labels"].append(label_entry)

        flat_classificaties = [
            {
                "document_id": r[0],
                "document_naam": r[1],
                "classificatie_code": r[2],
                "classificatie_label": r[3] or r[2],
                "confidence": r[4],
                "classified_at": str(r[5]) if r[5] else None,
                "status": r[6] or "concept",
                "image_description": r[7],
                "omschrijving": r[8],
                "toelichting": r[9],
            }
            for r in rows
        ]

        return {
            "zaak_id": zaak_id,
            "classificaties": flat_classificaties,
            "documenten": list(docs_map.values()),
        }
    finally:
        return_connection(conn)


class GoedkeurenInput(BaseModel):
    document_ids: Optional[List[int]] = None
    alle: bool = False


def _save_embedding_for_doc(zaak_id_int: int, doc_id: int, classificatie_code: str, cursor, conn):
    """Genereer en sla embedding op voor een goedgekeurd document."""
    try:
        cursor.execute(
            "SELECT extracted_text FROM leef_doc_classificatie WHERE zaak_id = %s AND document_id = %s LIMIT 1",
            (zaak_id_int, doc_id),
        )
        row = cursor.fetchone()
        if not row or not row[0]:
            return
        text = row[0]
        embedding = _generate_embedding(text)
        if not embedding:
            return
        embedding_json = _json.dumps(embedding)
        model = os.environ.get("OLLAMA_MODEL", "qwen2:0.5b")
        cursor.execute(
            """INSERT INTO doc_classificatie_embeddings
               (zaak_id, document_id, classificatie_code, embedding_vector, embedding_model, extracted_text_preview)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (zaak_id, document_id, classificatie_code) DO UPDATE SET
                   embedding_vector = EXCLUDED.embedding_vector,
                   embedding_model = EXCLUDED.embedding_model,
                   extracted_text_preview = EXCLUDED.extracted_text_preview,
                   created_at = CURRENT_TIMESTAMP""",
            (zaak_id_int, doc_id, classificatie_code, embedding_json, model, text[:500]),
        )
        conn.commit()
        logger.info(f"Embedding opgeslagen voor doc {doc_id} (zaak {zaak_id_int})")
    except Exception as e:
        logger.warning(f"Embedding opslaan mislukt voor doc {doc_id}: {e}")


def _send_labels_to_leef(zaak_id_int: int, doc_ids: List[int]) -> dict:
    """Stuur goedgekeurde classificatie-labels naar LEEF."""
    result = {"success": [], "failed": [], "errors": []}
    if not doc_ids:
        return result

    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        for doc_id in doc_ids:
            try:
                cursor.execute(
                    """SELECT lc.classificatie_code, dc.label, lc.doc_toelichting, lc.doc_omschrijving
                       FROM leef_doc_classificatie lc
                       LEFT JOIN doc_classificaties dc ON dc.code = lc.classificatie_code
                       WHERE lc.zaak_id = %s AND lc.document_id = %s AND lc.status = 'goedgekeurd'""",
                    (zaak_id_int, doc_id),
                )
                rows = cursor.fetchall()
                if not rows:
                    continue

                labels = [r[1] or r[0] for r in rows]
                toelichting = rows[0][2] or ""
                if len(toelichting) > 256:
                    toelichting = toelichting[:256]

                body = {"id": doc_id, "labels": labels}
                if toelichting:
                    body["toelichting"] = toelichting

                _leef_api_post("/documenten/labels", json_body=body)

                result["success"].append(doc_id)
                logger.info(f"LEEF labels verstuurd voor doc {doc_id} (zaak {zaak_id_int}): {labels}")

                try:
                    cursor.execute(
                        """UPDATE leef_doc_classificatie
                           SET leef_labels_sent_at = CURRENT_TIMESTAMP
                           WHERE zaak_id = %s AND document_id = %s AND status = 'goedgekeurd'""",
                        (zaak_id_int, doc_id),
                    )
                    conn.commit()
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:
                        pass

            except Exception as e:
                logger.warning(f"LEEF labels versturen mislukt voor doc {doc_id}: {e}")
                result["failed"].append(doc_id)
                result["errors"].append(f"Doc {doc_id}: {str(e)}")
    finally:
        return_connection(conn)

    return result


@router.post("/leef-datashare/zaken/{zaak_id}/classificaties/goedkeuren")
async def leef_datashare_classificaties_goedkeuren(zaak_id: str, data: GoedkeurenInput):
    """Keur classificaties goed: zet status van 'concept' naar 'goedgekeurd'."""
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        zaak_id_int = int(zaak_id) if zaak_id.isdigit() else 0
        docs_to_embed = []

        if data.alle:
            cursor.execute(
                """SELECT document_id, classificatie_code FROM leef_doc_classificatie
                   WHERE zaak_id = %s AND status = 'concept'""",
                (zaak_id_int,),
            )
            docs_to_embed = [(r[0], r[1]) for r in cursor.fetchall()]
            cursor.execute(
                """UPDATE leef_doc_classificatie
                   SET status = 'goedgekeurd'
                   WHERE zaak_id = %s AND status = 'concept'""",
                (zaak_id_int,),
            )
        elif data.document_ids:
            for doc_id in data.document_ids:
                cursor.execute(
                    """SELECT classificatie_code FROM leef_doc_classificatie
                       WHERE zaak_id = %s AND document_id = %s AND status = 'concept'""",
                    (zaak_id_int, doc_id),
                )
                row = cursor.fetchone()
                if row:
                    docs_to_embed.append((doc_id, row[0]))
                cursor.execute(
                    """UPDATE leef_doc_classificatie
                       SET status = 'goedgekeurd'
                       WHERE zaak_id = %s AND document_id = %s AND status = 'concept'""",
                    (zaak_id_int, doc_id),
                )
        else:
            raise HTTPException(status_code=400, detail="Geef document_ids of alle=true mee")
        conn.commit()
        updated = cursor.rowcount

        for doc_id, code in docs_to_embed:
            _save_embedding_for_doc(zaak_id_int, doc_id, code, cursor, conn)

        leef_labels_result = {"success": [], "failed": [], "errors": []}
        try:
            approved_doc_ids = list(set(d[0] for d in docs_to_embed))
            if approved_doc_ids:
                leef_labels_result = _send_labels_to_leef(zaak_id_int, approved_doc_ids)
        except Exception as e:
            logger.warning(f"LEEF labels versturen na goedkeuring mislukt: {e}")
            leef_labels_result["errors"].append(str(e))

        _invalidate_zaken_cache()
        return {"status": "ok", "zaak_id": zaak_id, "goedgekeurd": updated, "leef_labels": leef_labels_result}
    finally:
        return_connection(conn)


class WijzigClassificatieInput(BaseModel):
    classificatie_code: Optional[str] = None
    classificatie_codes: Optional[List[str]] = None
    reden: Optional[str] = None


@router.post("/leef-datashare/zaken/{zaak_id}/classificaties/{doc_id}/wijzig")
async def leef_datashare_classificatie_wijzig(zaak_id: str, doc_id: int, data: WijzigClassificatieInput):
    """Handmatig wijzigen van classificatie(s)."""
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        zaak_id_int = int(zaak_id) if zaak_id.isdigit() else 0

        new_codes = data.classificatie_codes or ([data.classificatie_code] if data.classificatie_code else [])
        if not new_codes:
            raise HTTPException(status_code=400, detail="Geef classificatie_code of classificatie_codes mee")

        cursor.execute(
            "SELECT document_naam, extracted_text, llm_response, image_description, samenvatting, doc_omschrijving, doc_toelichting, classificatie_code FROM leef_doc_classificatie WHERE zaak_id = %s AND document_id = %s",
            (zaak_id_int, doc_id),
        )
        existing_rows = cursor.fetchall()
        existing = existing_rows[0] if existing_rows else None
        doc_naam = existing[0] if existing else None
        extracted_text = existing[1] if existing else None
        llm_response = existing[2] if existing else None
        image_description = existing[3] if existing else None
        samenvatting = existing[4] if existing else None
        doc_omschrijving = existing[5] if existing else None
        doc_toelichting = existing[6] if existing else None
        originele_codes = sorted(set(r[7] for r in existing_rows if r[7]))

        cursor.execute(
            "DELETE FROM leef_doc_classificatie WHERE zaak_id = %s AND document_id = %s",
            (zaak_id_int, doc_id),
        )

        for code in new_codes:
            cursor.execute(
                """INSERT INTO leef_doc_classificatie
                   (zaak_id, document_id, document_naam, classificatie_code, confidence, llm_response, extracted_text, status, image_description, samenvatting, doc_omschrijving, doc_toelichting)
                   VALUES (%s, %s, %s, %s, 1.0, %s, %s, 'goedgekeurd', %s, %s, %s, %s)""",
                (zaak_id_int, doc_id, doc_naam, code, llm_response, extracted_text, image_description, samenvatting, doc_omschrijving, doc_toelichting),
            )

        if sorted(new_codes) != originele_codes:
            cursor.execute(
                """INSERT INTO classificatie_feedback
                   (zaak_id, document_id, document_naam, originele_codes, nieuwe_codes, reden)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (zaak_id_int, doc_id, doc_naam,
                 ','.join(originele_codes), ','.join(new_codes),
                 data.reden.strip() if data.reden and data.reden.strip() else None),
            )

        if data.reden and data.reden.strip():
            nieuwe_tip = data.reden.strip()
            for code in new_codes:
                cursor.execute(
                    "SELECT herkenning_tips FROM doc_classificaties WHERE code = %s",
                    (code,),
                )
                row = cursor.fetchone()
                if row is not None:
                    bestaande = row[0] or ""
                    if bestaande:
                        updated_tips = f"{bestaande}; {nieuwe_tip}"
                    else:
                        updated_tips = nieuwe_tip
                    cursor.execute(
                        "UPDATE doc_classificaties SET herkenning_tips = %s WHERE code = %s",
                        (updated_tips, code),
                    )

        conn.commit()

        _save_embedding_for_doc(zaak_id_int, doc_id, new_codes[0], cursor, conn)

        leef_labels_result = {"success": [], "failed": [], "errors": []}
        try:
            leef_labels_result = _send_labels_to_leef(zaak_id_int, [doc_id])
        except Exception as e:
            logger.warning(f"LEEF labels versturen na wijziging mislukt: {e}")
            leef_labels_result["errors"].append(str(e))

        _invalidate_zaken_cache()
        return {
            "status": "ok",
            "zaak_id": zaak_id,
            "document_id": doc_id,
            "classificatie_codes": new_codes,
            "classificatie_code": new_codes[0],
            "leef_labels": leef_labels_result,
        }
    finally:
        return_connection(conn)


@router.post("/leef-datashare/zaken/{zaak_id}/labels/verstuur")
async def leef_datashare_labels_verstuur(zaak_id: str, force: bool = False):
    """Handmatig (opnieuw) versturen van labels naar LEEF."""
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        zaak_id_int = int(zaak_id) if zaak_id.isdigit() else 0

        if force:
            cursor.execute(
                """SELECT DISTINCT document_id FROM leef_doc_classificatie
                   WHERE zaak_id = %s AND status = 'goedgekeurd'""",
                (zaak_id_int,),
            )
        else:
            try:
                cursor.execute(
                    """SELECT DISTINCT document_id FROM leef_doc_classificatie
                       WHERE zaak_id = %s AND status = 'goedgekeurd' AND leef_labels_sent_at IS NULL""",
                    (zaak_id_int,),
                )
            except Exception:
                conn.rollback()
                cursor.execute(
                    """SELECT DISTINCT document_id FROM leef_doc_classificatie
                       WHERE zaak_id = %s AND status = 'goedgekeurd'""",
                    (zaak_id_int,),
                )
        doc_ids = [r[0] for r in cursor.fetchall()]

        if not doc_ids:
            return {"status": "ok", "message": "Geen documenten om te versturen", "leef_labels": {"success": [], "failed": [], "errors": []}}

        result = _send_labels_to_leef(zaak_id_int, doc_ids)
        return {"status": "ok", "zaak_id": zaak_id, "leef_labels": result}
    finally:
        return_connection(conn)


@router.get("/leef-datashare/feedback")
async def leef_datashare_feedback(limit: int = 200, dagen: Optional[int] = None):
    """Alle classificatie-feedback rijen, nieuwste eerst."""
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        if dagen:
            cursor.execute(
                """SELECT id, zaak_id, document_id, document_naam, originele_codes, nieuwe_codes, reden, created_at
                   FROM classificatie_feedback
                   WHERE created_at >= NOW() - make_interval(days => %s)
                   ORDER BY created_at DESC LIMIT %s""",
                (dagen, limit),
            )
        else:
            cursor.execute(
                """SELECT id, zaak_id, document_id, document_naam, originele_codes, nieuwe_codes, reden, created_at
                   FROM classificatie_feedback
                   ORDER BY created_at DESC LIMIT %s""",
                (limit,),
            )
        rows = cursor.fetchall()
        return [
            {
                "id": r[0], "zaak_id": r[1], "document_id": r[2],
                "document_naam": r[3], "originele_codes": r[4],
                "nieuwe_codes": r[5], "reden": r[6],
                "created_at": r[7].isoformat() if r[7] else None,
            }
            for r in rows
        ]
    finally:
        return_connection(conn)


@router.get("/leef-datashare/feedback/stats")
async def leef_datashare_feedback_stats(dagen: Optional[int] = None):
    """Statistieken: telling per originele->nieuwe code combinatie."""
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        if dagen:
            cursor.execute(
                """SELECT originele_codes, nieuwe_codes, COUNT(*) as aantal
                   FROM classificatie_feedback
                   WHERE created_at >= NOW() - make_interval(days => %s)
                   GROUP BY originele_codes, nieuwe_codes
                   ORDER BY aantal DESC""",
                (dagen,),
            )
        else:
            cursor.execute(
                """SELECT originele_codes, nieuwe_codes, COUNT(*) as aantal
                   FROM classificatie_feedback
                   GROUP BY originele_codes, nieuwe_codes
                   ORDER BY aantal DESC"""
            )
        rows = cursor.fetchall()
        return [
            {"originele_codes": r[0], "nieuwe_codes": r[1], "aantal": r[2]}
            for r in rows
        ]
    finally:
        return_connection(conn)


@router.get("/leef-datashare/documenten/{doc_id}/metadata")
async def leef_datashare_document_metadata(doc_id: int):
    """Haal document metadata incl. bestaande LEEF labels op."""
    response = _leef_api_get(f"/documenten/metadata/{doc_id}")
    return response.json()


@router.get("/leef-datashare/labels")
async def leef_datashare_labels():
    """Haal alle beschikbare labels in LEEF op."""
    response = _leef_api_get("/documenten/labels")
    return response.json()


class LeefLabelsInput(BaseModel):
    labels: List[str]
    toelichting: Optional[str] = Field(None, max_length=256)


@router.post("/leef-datashare/documenten/{doc_id}/labels")
async def leef_datashare_document_labels_post(doc_id: int, data: LeefLabelsInput):
    """Stuur labels naar LEEF voor een specifiek document."""
    body = {"id": doc_id, "labels": data.labels}
    if data.toelichting:
        body["toelichting"] = data.toelichting
    response = _leef_api_post("/documenten/labels", json_body=body)
    return response.json()


@router.get("/leef-datashare/zaken/{zaak_id}/review")
async def leef_datashare_zaak_review(zaak_id: str):
    """Combineer alle data voor de review-pagina in 1 call."""
    try:
        response = _leef_api_get(f"/zaken/{zaak_id}")
        data = response.json()
        if isinstance(data, dict) and "items" in data and data["items"]:
            zaak = data["items"][0]
        elif isinstance(data, list) and data:
            zaak = data[0]
        else:
            zaak = data if isinstance(data, dict) else {}

        documenten = zaak.get("documenten", [])

        conn = get_connection()
        cursor = get_cursor(conn)
        classificaties_map = {}
        codes = []
        try:
            zaak_id_int = int(zaak_id) if zaak_id.isdigit() else 0
            cursor.execute(
                """SELECT lc.document_id, lc.document_naam, lc.classificatie_code,
                          dc.label AS classificatie_label, lc.confidence, lc.status,
                          lc.llm_response, lc.image_description, lc.samenvatting,
                          lc.doc_omschrijving, lc.doc_toelichting
                   FROM leef_doc_classificatie lc
                   LEFT JOIN doc_classificaties dc ON dc.code = lc.classificatie_code
                   WHERE lc.zaak_id = %s
                   ORDER BY lc.document_id, lc.confidence DESC""",
                (zaak_id_int,),
            )
            class_rows = cursor.fetchall()
            for r in class_rows:
                doc_id_key = r[0]
                label_entry = {
                    "document_id": r[0],
                    "document_naam": r[1],
                    "classificatie_code": r[2],
                    "classificatie_label": r[3] or r[2],
                    "confidence": r[4],
                    "status": r[5] or "concept",
                    "llm_response": r[6],
                    "image_description": r[7],
                    "samenvatting": r[8],
                    "omschrijving": r[9],
                    "toelichting": r[10],
                }
                if doc_id_key not in classificaties_map:
                    classificaties_map[doc_id_key] = []
                classificaties_map[doc_id_key].append(label_entry)

            cursor.execute(
                "SELECT code, label, omschrijving FROM doc_classificaties WHERE actief = true ORDER BY volgorde"
            )
            codes = [{"code": r[0], "label": r[1], "omschrijving": r[2]} for r in cursor.fetchall()]
        finally:
            return_connection(conn)

        docs_enriched = []
        for doc in documenten:
            doc_id = doc.get("id")
            cls_list = classificaties_map.get(doc_id, classificaties_map.get(int(doc_id) if str(doc_id).isdigit() else -1))
            first_cls = cls_list[0] if cls_list else None
            docs_enriched.append({
                "id": doc_id,
                "naam": doc.get("naam") or doc.get("name") or "Document",
                "mime_type": doc.get("mime_type", ""),
                "classificaties": cls_list or [],
                "classificatie": first_cls,
            })

        totaal = len(documenten)
        geclassificeerd = sum(1 for d in docs_enriched if d["classificaties"])
        goedgekeurd = sum(
            1 for d in docs_enriched
            if d["classificaties"] and all(c["status"] == "goedgekeurd" for c in d["classificaties"])
        )

        teag_code = str(zaak.get("TEAG_CODE") or zaak.get("teag_code") or "").strip().upper()
        type_verg = zaak.get("TYPE_VERGUNNING") or zaak.get("type_vergunning") or ""
        zaaktype_omschr = type_verg
        if teag_code and teag_code in _ZAAKTYPE_MAPPING:
            zaaktype_omschr = _ZAAKTYPE_MAPPING[teag_code][0]

        straat = zaak.get("STRAATNAAM") or zaak.get("straatnaam") or ""
        huisnr = zaak.get("huisnummer") or zaak.get("HUISNUMMER") or ""
        woonpl = zaak.get("woonplaats") or zaak.get("WOONPLAATS") or ""
        locatie_str = zaak.get("locatie") or ""
        if straat and not locatie_str:
            locatie_str = f"{straat} {huisnr}".strip()
            if woonpl:
                locatie_str += f", {woonpl}"

        return {
            "zaak_id": zaak_id,
            "zaak": {
                "intern_kenmerk": zaak.get("INTERN_KENMERK") or zaak.get("intern_kenmerk"),
                "zaak_omschrijving": zaak.get("Zaakomschrijving") or zaak.get("zaak_omschrijving") or zaak.get("zaakomschrijving"),
                "zaaktype": teag_code or zaak.get("zaaktype"),
                "zaaktype_omschrijving": zaaktype_omschr,
                "type_vergunning": type_verg,
                "locatie": locatie_str,
                "datum_aanvraag": zaak.get("DATUM_AANVRAAG") or zaak.get("datum_aanvraag"),
                "status": zaak.get("status"),
            },
            "documenten": docs_enriched,
            "classificatie_codes": codes,
            "progress": {
                "totaal": totaal,
                "geclassificeerd": geclassificeerd,
                "goedgekeurd": goedgekeurd,
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Review endpoint fout voor zaak {zaak_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Review data ophalen mislukt: {str(e)}")
