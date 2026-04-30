from __future__ import annotations
import logging
import json
from datetime import timedelta, datetime, timezone

import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text

logger = logging.getLogger(__name__)
start_date = datetime.now(timezone.utc) - timedelta(days=1)

# Villes à surveiller — modifiable sans toucher au code via Airflow Variables
DEFAULT_CITIES = ["Paris", "Abidjan", "New York", "Berlin", "Tokyo"]

# Paramètres par défaut appliqués à chaque tâche du DAG
DEFAULT_ARGS = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "email": ["alerts@monentreprise.com"],
    "email_on_failure": False,   # désactivé : pas de SMTP configuré en local
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=3),
}


## ── TASK 1 : EXTRACT ────────────────────────────────────────────────────
def extract_weather_data(**context) -> None:
    """
    Extraire les données météo de chaque ville via l'API OpenWeather.
    Pousse les données brutes via XCom à la tâche suivante.
    """

    ## 1- récupérer la liste des villes depuis Airflow Variables
    try:
        cities_json = Variable.get("weather_cities")
        cities = json.loads(cities_json)
    except Exception:
        cities = DEFAULT_CITIES
        logger.info(f"Variable 'weather_cities' absente -- utilisation de {cities}")

    ## 2- récupérer la clé API
    api_key = Variable.get("openweather_api_key", default_var=None)
    if not api_key:
        raise ValueError("Variable d'airflow 'openweather_api_key' manquante")

    ## HttpHook utilise la connection 'openweather_api' configurée dans l'UI
    http_hook = HttpHook(method="GET", http_conn_id="openweather_api")

    raw_data = []
    errors = []

    for city in cities:
        try:
            logger.info(f"requête api pour : {city}")

            endpoint = (
                f"/data/2.5/weather"
                f"?q={city}&appid={api_key}&units=metric&lang=fr"
            )

            response = http_hook.run(endpoint)
            data = response.json()

            if response.status_code != 200:
                logger.warning(f"{city} : status {response.status_code}")
                errors.append({"city": city, "error": data.get("message", "Unknown")})
                continue

            raw_data.append(data)
            logger.info(f"{city} : {data['main']['temp']}°C")

        except Exception as e:
            logger.error(f"erreur pour {city} : {e}")
            errors.append({"city": city, "error": str(e)})

    if not raw_data:
        raise ValueError(f"aucune donnée extraite. erreurs : {errors}")

    logger.info(f"Extract terminée : {len(raw_data)} villes OK | {len(errors)} erreurs")

    context["ti"].xcom_push(key="raw_weather", value=raw_data)
    context["ti"].xcom_push(key="extract_errors", value=errors)


## ── TASK 2 : TRANSFORM ──────────────────────────────────────────────────
def transform_weather_data(**context) -> None:
    """
    Transforme les données brutes JSON en DataFrame structuré.
    Validation et normalisation des champs.
    """

    raw_data = context["ti"].xcom_pull(task_ids="extract", key="raw_weather")

    if not raw_data:
        raise ValueError("aucune donnée brute reçue de la tâche extract")

    records = []

    for item in raw_data:
        try:
            record = {
                "city": item["name"],
                "country_code": item["sys"]["country"],
                "temperature": round(item["main"]["temp"], 2),
                "feels_like": round(item["main"]["feels_like"], 2),
                "humidity": item["main"]["humidity"],
                "pressure": item["main"]["pressure"],
                "wind_speed": round(item["wind"]["speed"], 2),
                "weather_main": item["weather"][0]["main"],
                "description": item["weather"][0]["description"],
                "collected_at": datetime.utcfromtimestamp(item["dt"]),
            }
            records.append(record)

        except KeyError as e:
            logger.warning(f"champ manquant dans la réponse api : {e}")
            continue

    if not records:
        raise ValueError("Aucun enregistrement valide après transformation")

    df = pd.DataFrame(records)

    ## validations
    assert df["temperature"].between(-80, 60).all(), "Température hors plage réelle"
    assert df["humidity"].between(0, 100).all(), "humidité hors plage [0-100]"

    logger.info(f"transformation OK : {len(df)} enregistrements")
    logger.info(f"\n{df[['city', 'temperature', 'humidity', 'weather_main']].to_string()}")

    context["ti"].xcom_push(key="clean_weather", value=df.to_json(date_format="iso"))


## ── TASK 3 : LOAD ───────────────────────────────────────────────────────
def load_weather_data(**context) -> None:
    """
    Charge les données transformées dans PostgreSQL.
    """

    df_json = context["ti"].xcom_pull(task_ids="transform", key="clean_weather")
    df = pd.read_json(df_json)

    if df.empty:
        logger.warning("dataframe vide, aucun chargement")
        return

    ## ajouter le timestamp de chargement
    df["loaded_at"] = datetime.utcnow()

    pg_hook = PostgresHook(postgres_conn_id="postgres_etl")
    engine = pg_hook.get_sqlalchemy_engine()

    df.to_sql(
        name="weather_data",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )

    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM weather_data")).scalar()

    logger.info(f"load OK : {len(df)} lignes chargées || total en base {total}")

    context["ti"].xcom_push(key="rows_loaded", value=len(df))


## ── TASK 4 : QUALITY CHECK ──────────────────────────────────────────────
def quality_check(**context) -> None:
    """
    Vérifie la qualité des données chargées.
    Lève une exception en cas d'anomalies.
    """

    pg_hook = PostgresHook(postgres_conn_id="postgres_etl")

    ## fenêtre de temps : 1 heure avant l'execution_date
    execution_date = context["execution_date"]
    window_start = execution_date - timedelta(hours=1)
    logger.info(f"contrôle qualité depuis {window_start}")

    checks = {
        "no_duplicates": """
            SELECT COUNT(*) FROM (
                SELECT city, collected_at, COUNT(*)
                FROM weather_data
                WHERE loaded_at >= NOW() - INTERVAL '1 hour'
                GROUP BY city, collected_at
                HAVING COUNT(*) > 1
            ) t
        """,
        "valid_temperature": """
            SELECT COUNT(*) FROM weather_data
            WHERE loaded_at >= NOW() - INTERVAL '1 hour'
              AND (temperature < -80 OR temperature > 60)
        """,
    }

    for check_name, query in checks.items():
        result = pg_hook.get_first(query)[0]
        if result > 0:
            raise ValueError(
                f"vérification de la qualité a échoué [{check_name}] : {result} anomalies"
            )
        logger.info(f"quality check OK : {check_name}")


## ── DÉFINITION DU DAG ───────────────────────────────────────────────────
with DAG(
    dag_id="weather_etl_pipeline",
    description="Collecte météo API → PostgreSQL (toutes les heures)",
    default_args=DEFAULT_ARGS,
    start_date=start_date,
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["etl", "weather", "api", "formation"],
) as dag:

    t_extract = PythonOperator(task_id="extract", python_callable=extract_weather_data)
    t_transform = PythonOperator(task_id="transform", python_callable=transform_weather_data)
    t_load = PythonOperator(task_id="load", python_callable=load_weather_data)
    t_quality = PythonOperator(task_id="quality_check", python_callable=quality_check)

    t_extract >> t_transform >> t_load >> t_quality








