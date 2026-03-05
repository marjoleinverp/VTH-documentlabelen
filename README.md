# VTH Document Labelen

Automatische documentclassificatie voor VTH-zaken (Vergunningen, Toezicht, Handhaving) via de LEEF Datashare API. Documenten worden door een lokaal AI-model (Ollama) geanalyseerd en voorzien van classificatielabels.

## Wat doet het?

1. **Zaken ophalen** uit LEEF Datashare (via OAuth2)
2. **Documenten classificeren** met een LLM (Ollama of Azure OpenAI)
   - PDF-tekst wordt geextraheerd via pdfplumber
   - Afbeeldingen/scans worden geanalyseerd via een vision-model
   - Multi-label classificatie met confidence score
3. **Review workflow** - classificaties starten als "concept" en worden handmatig goedgekeurd
4. **Labels terugsturen** naar de LEEF API na goedkeuring
5. **Lerend systeem** - embeddings van goedgekeurde classificaties verbeteren toekomstige suggesties

## Snel starten

### Vereisten

- Docker en Docker Compose
- LEEF API credentials (OAuth2 client credentials)
- Minimaal 4 GB RAM beschikbaar voor Ollama

### Installatie

```bash
git clone https://github.com/marjoleinverp/VTH-documentlabelen.git
cd VTH-documentlabelen

# Configuratie aanmaken
cp .env.example .env
# Vul minimaal in: POSTGRES_PASSWORD, LEEF_API_BASE_URL, LEEF_CLIENT_ID,
#                  LEEF_CLIENT_SECRET, LEEF_TOKEN_URL

# Starten
docker-compose up -d
```

De applicatie is beschikbaar op http://localhost:8000

### Ollama modellen laden

Bij de eerste keer starten moeten de AI-modellen nog gedownload worden. Dit kan even duren (eenmalig):

```bash
# Tekstmodel voor classificatie + embeddings (~350MB)
docker-compose exec ollama ollama pull qwen2:0.5b

# Vision-model voor afbeeldingen/scans (~2GB)
docker-compose exec ollama ollama pull llama3.2-vision
```

## Architectuur

```
docker-compose.yml
  +-- app (FastAPI, poort 8000)
  |     Documentclassificatie, review workflow, LEEF API koppeling
  +-- db (PostgreSQL 16)
  |     Classificaties, embeddings, feedback, prompts
  +-- ollama (Ollama LLM)
        Lokale AI-modellen voor tekst en vision
```

### Pagina's

| Pagina | Beschrijving |
|--------|-------------|
| `/` | Hoofdoverzicht - zakenlijst met classificatiestatus |
| `/static/classificatie_review.html` | Split-view document review met preview |
| `/static/classificatie_beheer.html` | Classificatiecategorieen beheren |
| `/static/classificatie_feedback.html` | Feedback en KPI's |
| `/static/classificatie_prompts.html` | LLM prompts aanpassen |

### API endpoints

Alle endpoints staan onder `/geo/leef-datashare/`:

| Methode | Pad | Beschrijving |
|---------|-----|-------------|
| GET | `/zaken` | Zakenlijst met classificatiestatus |
| GET | `/zaken/{id}` | Zaak detail met documenten |
| POST | `/zaken/{id}/classificeer` | Classificeer alle documenten |
| POST | `/zaken/{id}/classificeer/{doc_id}` | Classificeer enkel document |
| GET | `/zaken/{id}/classificaties` | Opgeslagen classificaties per zaak |
| POST | `/zaken/{id}/classificaties/goedkeuren` | Concept -> goedgekeurd |
| POST | `/zaken/{id}/classificaties/{doc_id}/wijzig` | Handmatig wijzigen |
| POST | `/zaken/{id}/labels/verstuur` | Labels naar LEEF sturen |
| GET | `/classificaties` | Alle classificatiecategorieen |
| POST | `/classificaties` | Categorie toevoegen/wijzigen |
| GET | `/prompts` | LLM prompts ophalen |
| POST | `/prompts` | Prompt opslaan |
| GET | `/feedback` | Feedback overzicht |
| GET | `/feedback/stats` | Feedback statistieken |
| GET | `/health` | Health check |

## Configuratie

Zie `.env.example` voor alle variabelen. Minimaal vereist:

| Variabele | Beschrijving |
|-----------|-------------|
| `POSTGRES_PASSWORD` | Wachtwoord voor de database |
| `LEEF_API_BASE_URL` | Base URL van de LEEF Datashare API |
| `LEEF_CLIENT_ID` | OAuth2 client ID voor LEEF API |
| `LEEF_CLIENT_SECRET` | OAuth2 client secret voor LEEF API |
| `LEEF_TOKEN_URL` | OAuth2 token endpoint (Microsoft Entra ID) |

### GPU-ondersteuning (optioneel)

Voor snellere classificatie met een NVIDIA GPU, uncomment het `deploy` blok in `docker-compose.yml` bij de `ollama` service. Vereist de [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html).

### Azure OpenAI (optioneel)

Als alternatief voor of aanvulling op Ollama kan Azure OpenAI gebruikt worden. Vul de `AZURE_OPENAI_*` variabelen in de `.env`. De app probeert eerst Azure OpenAI, dan Ollama als fallback.

## Ontwikkeling

### Projectstructuur

```
app/
  api.py              # FastAPI app entry point
  leef_routes.py      # Alle LEEF datashare routes
  db_config.py        # PostgreSQL connection pooling
  init_database.py    # Database tabellen en seed data
  zaaktype_mapping.py # Zaaktype code -> naam mapping
  logging_config.py   # Structured logging
static/
  leef_datashare.html          # Hoofdpagina
  classificatie_review.html    # Document review
  classificatie_beheer.html    # Categorie beheer
  classificatie_feedback.html  # Feedback dashboard
  classificatie_prompts.html   # Prompt editor
```

### Database tabellen

| Tabel | Beschrijving |
|-------|-------------|
| `doc_classificaties` | Classificatiecategorieen (33 standaard) |
| `leef_doc_classificatie` | Resultaten per document (multi-label) |
| `doc_classificatie_embeddings` | Embeddings voor lerend systeem |
| `classificatie_feedback` | Gebruikersfeedback |
| `classificatie_prompts` | Aanpasbare LLM prompts |

Tabellen worden automatisch aangemaakt bij opstarten.

## Status: Pilot

Deze applicatie is een werkend pilot/MVP. Onderstaande verbeteringen zijn nodig om van pilot naar productie te gaan.

### Moet (voor productie)

- [ ] **Authenticatie toevoegen** - alle endpoints zijn nu open. Implementeer OAuth2 Bearer tokens of API key authenticatie zodat alleen geautoriseerde gebruikers zaken kunnen inzien, classificaties kunnen goedkeuren en labels kunnen versturen
- [ ] **CORS restrictief configureren** - staat nu op `*` (alle origins). Beperk tot specifieke toegestane domeinen
- [ ] **Foutafhandeling bij Ollama failure** - als het LLM niet bereikbaar is, krijgen documenten nu stilzwijgend het label "bijlage". Dit moet een duidelijke foutmelding worden
- [ ] **Database startup failure afvangen** - als de database niet bereikbaar is bij opstarten, draait de app nu gewoon door. Moet blokkeren of health endpoint op unhealthy zetten

### Zou moeten (voor stabiliteit)

- [ ] **Database context manager gebruiken** - er is een `get_db_connection()` context manager met rollback-logica, maar de routes gebruiken deze nog niet. Overstappen voorkomt connection leaks bij errors
- [ ] **Resource limits in Docker** - geheugen- en CPU-limieten toevoegen aan docker-compose, met name voor Ollama (LLM modellen kunnen veel geheugen gebruiken)
- [ ] **Dockerfile hardenen** - non-root user, gepinde image versie (`python:3.11.x-slim`), `.dockerignore` toevoegen
- [ ] **Environment variabelen opschonen** - de code gebruikt `OLLAMA_URL`, `LEEF_DATASHARE_URL`, `LEEF_OAUTH_TOKEN_URL` maar de `.env` noemt ze anders. De docker-compose mapt dit nu dubbel

### Kan (voor productie-kwaliteit)

- [ ] **Request tracing** - correlation ID per request voor debuggen in productie
- [ ] **Stack traces bij exceptions** - `exc_info=True` toevoegen aan error logging
- [ ] **Rate limiting** - op zware endpoints zoals classificeer (LLM calls)
- [ ] **Monitoring** - Prometheus metrics of OpenTelemetry voor health dashboards

## Licentie

Intern project Gemeente Meierijstad.
