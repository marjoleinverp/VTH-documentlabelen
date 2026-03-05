"""
Initialiseer LEEF Datashare database tabellen - PostgreSQL
"""
from db_config import get_connection, get_cursor, check_database_health
from logging_config import get_logger

logger = get_logger(__name__)


def init_leef_tables():
    """Maak LEEF datashare tabellen aan (idempotent)."""
    conn = get_connection()
    cursor = get_cursor(conn)

    logger.info("Aanmaken LEEF datashare tabellen...")

    # Classificatie-categorieen (beheerbaar)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS doc_classificaties (
            id SERIAL PRIMARY KEY,
            code TEXT UNIQUE NOT NULL,
            label TEXT NOT NULL,
            omschrijving TEXT,
            actief BOOLEAN DEFAULT true,
            volgorde INTEGER DEFAULT 0,
            herkenning_tips TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    logger.info("  doc_classificaties OK")

    # Classificatie resultaten per document (multi-label)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS leef_doc_classificatie (
            id SERIAL PRIMARY KEY,
            zaak_id INTEGER NOT NULL,
            document_id INTEGER NOT NULL,
            document_naam TEXT,
            classificatie_code TEXT,
            confidence REAL,
            llm_response TEXT,
            extracted_text TEXT,
            status TEXT DEFAULT 'concept',
            image_description TEXT,
            samenvatting TEXT,
            doc_omschrijving TEXT,
            doc_toelichting TEXT,
            leef_labels_sent_at TIMESTAMP,
            classified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(zaak_id, document_id, classificatie_code)
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_leef_doc_class_zaak ON leef_doc_classificatie(zaak_id)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_leef_doc_class_zaak_status
          ON leef_doc_classificatie(zaak_id, status)
    """)
    logger.info("  leef_doc_classificatie OK")

    # Classificatie embeddings (voor lerend systeem, multi-label)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS doc_classificatie_embeddings (
            id SERIAL PRIMARY KEY,
            zaak_id INTEGER NOT NULL,
            document_id INTEGER NOT NULL,
            classificatie_code TEXT NOT NULL,
            embedding_vector TEXT NOT NULL,
            embedding_model TEXT,
            extracted_text_preview TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(zaak_id, document_id, classificatie_code)
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_class_emb_code ON doc_classificatie_embeddings(classificatie_code)
    """)
    logger.info("  doc_classificatie_embeddings OK")

    # Classificatie feedback logtabel
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS classificatie_feedback (
            id SERIAL PRIMARY KEY,
            zaak_id INTEGER NOT NULL,
            document_id INTEGER NOT NULL,
            document_naam TEXT,
            originele_codes TEXT NOT NULL,
            nieuwe_codes TEXT NOT NULL,
            reden TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_class_feedback_zaak ON classificatie_feedback(zaak_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_class_feedback_created ON classificatie_feedback(created_at)")
    logger.info("  classificatie_feedback OK")

    # Classificatie-prompts tabel
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS classificatie_prompts (
            key VARCHAR(50) PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    logger.info("  classificatie_prompts OK")

    # Seed classificaties
    seed_classificaties = [
        ('aanvraagformulier', 'Aanvraagformulier', 'Officieel aanvraag- of meldingsformulier', 1),
        ('bouwtekening_plattegrond', 'Bouwtekening - plattegrond', 'Technische plattegrondtekening van een bouwwerk', 2),
        ('bouwtekening_gevelaanzicht', 'Bouwtekening - gevelaanzicht', 'Technische geveltekening van een bouwwerk', 3),
        ('situatietekening', 'Situatietekening', 'Tekening van de situatie/locatie op het terrein', 4),
        ('constructieberekening', 'Constructieberekening', 'Berekeningen voor de constructieve veiligheid', 5),
        ('funderingstekening', 'Funderingstekening', 'Technische tekening van de fundering', 6),
        ('bestektekening', 'Bestektekening', 'Gedetailleerde uitvoeringstekening', 7),
        ('tekening_inrit', 'Tekening inrit', 'Tekening van de inrit/uitrit', 8),
        ('foto_bestaand', 'Foto bestaande situatie', 'Foto van de huidige/bestaande situatie', 9),
        ('nieuwe_situatie', 'Nieuwe situatie', 'Tekening of beschrijving van de nieuwe situatie', 10),
        ('bestaande_situatie', 'Bestaande situatie', 'Tekening of beschrijving van de bestaande situatie', 11),
        ('ruimtelijke_onderbouwing', 'Ruimtelijke onderbouwing', 'Onderbouwing van ruimtelijke aspecten', 12),
        ('bestemmingsplankaart', 'Bestemmingsplankaart', 'Kaart van het bestemmingsplan', 13),
        ('landschappelijke_inpassing', 'Landschappelijke inpassing', 'Plan voor landschappelijke inpassing', 14),
        ('herplantplan', 'Herplantplan', 'Plan voor herplant van bomen/groen', 15),
        ('boominventarisatie', 'Boominventarisatie', 'Inventarisatie van bomen op de locatie', 16),
        ('boomveiligheidscontrole', 'Boomveiligheidscontrole', 'Veiligheidscontrole van bomen', 17),
        ('ecologisch_onderzoek', 'Ecologisch onderzoek', 'Onderzoek naar ecologische effecten (flora/fauna)', 18),
        ('milieuonderzoek', 'Milieuonderzoek (geluid, bodem, lucht)', 'Onderzoek naar milieu-effecten', 19),
        ('geluidsonderzoek', 'Geluidsonderzoek', 'Akoestisch onderzoek', 20),
        ('archeologisch_rapport', 'Archeologisch rapport', 'Rapport over archeologische waarden', 21),
        ('cultuurhistorische_analyse', 'Cultuurhistorische analyse', 'Analyse van cultuurhistorische waarden', 22),
        ('bouwbesluittoets', 'Bouwbesluittoets', 'Toetsing aan het Bouwbesluit', 23),
        ('brandveiligheid', 'Brandveiligheid', 'Rapport of advies over brandveiligheid', 24),
        ('duurzaamheidsmaatregelen', 'Duurzaamheidsmaatregelen', 'Overzicht van duurzaamheidsmaatregelen (EPC, BENG)', 25),
        ('welstandsadvies', 'Welstandsadvies', 'Advies van de welstandscommissie', 26),
        ('afwijkingsverzoek', 'Afwijkingsverzoek', 'Verzoek tot afwijking van regelgeving', 27),
        ('advies_bijgevoegd', 'Advies bijgevoegd', 'Extern advies als bijlage', 28),
        ('beoordeling_vereist', 'Beoordeling vereist', 'Document dat handmatige beoordeling vereist', 29),
        ('concept', 'Concept', 'Conceptversie van een document', 30),
        ('definitief', 'Definitief', 'Definitieve versie van een document', 31),
        ('niet_relevant', 'Niet relevant', 'Document is niet relevant voor de aanvraag', 32),
        ('vervallen', 'Vervallen', 'Document is vervallen/niet meer geldig', 33),
    ]
    for code, label, omschrijving, volgorde in seed_classificaties:
        cursor.execute(
            """INSERT INTO doc_classificaties (code, label, omschrijving, volgorde)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (code) DO UPDATE SET
                   label = EXCLUDED.label,
                   omschrijving = EXCLUDED.omschrijving,
                   volgorde = EXCLUDED.volgorde""",
            (code, label, omschrijving, volgorde)
        )
    logger.info(f"  {len(seed_classificaties)} classificaties gesynchroniseerd")

    # Default prompts
    _default_prompts = [
        ('vision_prompt', 'Beschrijf wat je ziet in deze afbeelding. Is het een bouwtekening, situatieschets, plattegrond, doorsnede, gevelaanzicht, foto, formulier, brief of iets anders? Als het een technische tekening is, beschrijf dan de afmetingen, schaal, tekeningnummer en andere details die je kunt lezen. Antwoord in het Nederlands.'),
        ('classificatie_prompt', """Classificeer dit document in de onderstaande categorieen en geef een korte samenvatting, omschrijving en toelichting.
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
Let op: "niet_relevant" en "vervallen" mogen NIET gecombineerd worden met andere codes. Gebruik deze alleen als het document echt niet relevant of vervallen is."""),
        ('system_message', 'Je bent een document classificatie-assistent voor een Nederlandse gemeente. Antwoord altijd in het gevraagde JSON formaat.'),
    ]
    for key, value in _default_prompts:
        cursor.execute(
            """INSERT INTO classificatie_prompts (key, value)
               VALUES (%s, %s)
               ON CONFLICT (key) DO NOTHING""",
            (key, value),
        )
    logger.info("  Classificatie-prompts gesynchroniseerd")

    conn.commit()
    from db_config import return_connection
    return_connection(conn)

    logger.info("LEEF datashare database geinitialiseerd!")


if __name__ == "__main__":
    init_leef_tables()
