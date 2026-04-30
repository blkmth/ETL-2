
# ETL(EXTRACT , TRANSFORM , LOAD) NIVEAU INTERMEDIAIRE

J'ai eu pour projet de construire un pipeline qui allait extraire des données méteo de differente ville depuis openweather via une api, standardisé ces données les transformer et les stocker dansune base de donnée  postgresql. 


## Objectifs :
    
    - Installer et configurer Apache Airflow via Docker
    - Consommer une API REST avec le HttpHook Airflow
    - Créer un DAG structuré avec gestion des erreurs et retries
    - Monitorer les exécutions via l'interface web Airflow
    - Comprendre les concepts clés : DAG, Task, XCom, Connection



## Prerquis:

   - Docker Desktop installé et démarré
   - Docker Compose v2.x
   - Une clé API OpenWeatherMap (plan gratuit suffisant) → [Créer un compte](https://openweathermap.org/api)
   - Une base PostgreSQL locale avec une base `etl_formation` existante



## Structure

    
```
etl_intermediaire/
│
├── dags/
│   └── weather_etl_dag.py       # DAG principal Airflow
│
├── plugins/
│   └── operators/
│       └── weather_operator.py  # Opérateur custom (optionnel)
│
├── sql/
│   └── create_tables.sql        # Schéma PostgreSQL
│
├── docker-compose.yml           # Stack Airflow + PostgreSQL
├── .env                         # Variables d'environnement (non commité)
└── requirements.txt             # Dépendances Python custom
```


## Etapes Suivie 

### 1. Setup de l'environnement Docker
Première chose : faire tourner Airflow via Docker Compose plutôt qu'une installation locale. Ça évite les conflits de dépendances et c'est plus proche d'un environnement de production.

Le `docker-compose.yml` monte 5 services : 
    
    - le webserver Airflow, 
    - le scheduler, 
    - PostgreSQL (port 5433 pour ne pas entrer en conflit avec le PostgreSQL local sur 5432).
notons que nouvons deux services postgre une base pour `airflow` et une autre ou seront `stockées les données`.

#### 1.1.   Initialisation de la base de metadonnée airflow

```bash
  airflow-init
```
#### 1.2.   Lancer le conteneur
```bash
  docker-compose up -d
```
#### 1.3.   webserver airflow
```bash
  L'interface Airflow est accessible sur : http://localhost:8080
```

## Configuration dans l'interface Airflow
 
### Connections (Admin → Connections → +)
 
**OpenWeatherMap API**
 
| Champ | Valeur |
|-------|--------|
| Connection ID | `openweather_api` |
| Connection Type | HTTP |
| Host | `https://api.openweathermap.org` |
| Schema | `https` |
 
**PostgreSQL cible**
 
| Champ | Valeur |
|-------|--------|
| Connection ID | `postgres_etl` |
| Connection Type | Postgres |
| Host | `host.docker.internal` |
| Schema | `etl_formation` |
| Login | `postgres` |
| Password | ton_mot_de_passe |
| Port | `5432` |
 
### Variables (Admin → Variables → +)
 
| Key | Value |
|-----|-------|
| `openweather_api_key` | ta clé API OpenWeatherMap |
| `weather_cities` | `["Paris","Abidjan","Berlin","Tokyo","Montreal"]` |
 
---
 
## Lancer le pipeline
 
1. Ouvrir http://localhost:8080
2. Trouver le DAG `weather_etl_pipeline`
3. Activer le toggle **ON**
4. Cliquer sur **▶ Trigger DAG** pour un run manuel
Le DAG se déclenche automatiquement **toutes les heures** (`@hourly`).
 

### 2. Préparation de la base de données cible
 
Création de la table `weather_data` avec le script `sql/create_tables.sql`. Les champs couvrent la localisation, les températures, les conditions atmosphériques, et deux timestamps : `collected_at` (heure de la mesure côté API) et `loaded_at` (heure d'insertion en base).

**create_tables**

```bash
  psql -U postgres -d etl_formation -f sql/create_tables.sql
```
 
### 3. Configuration des Connections Airflow
 
Plutôt que de mettre les credentials en dur dans le code, j'ai utilisé le système de Connections d'Airflow (Admin → Connections) :
- `openweather_api` : connection HTTP vers l'API météo
- `postgres_etl` : connection vers ma base PostgreSQL locale
Les villes à surveiller et la clé API sont stockées dans les **Variables Airflow** — modifiables sans toucher au code.
 
### 4. Construction du DAG en 4 tâches
 
Le DAG suit le pattern ETL classique :
 
```
extract → transform → load → quality_check
```
 
| Tâche | Rôle |
|-------|------|
| `extract` | Appelle l'API OpenWeatherMap pour chaque ville |
| `transform` | Normalise les données JSON en DataFrame structuré |
| `load` | Insère les données dans la table `weather_data` |
| `quality_check` | Vérifie les doublons et la cohérence des données |

 
- **Extract** : appel API via `HttpHook` pour chaque ville, données brutes poussées dans XCom
- **Transform** : normalisation JSON → DataFrame pandas, avec assertions de validation (températures dans une plage réaliste, humidité entre 0 et 100)
- **Load** : insertion dans PostgreSQL via `PostgresHook` et SQLAlchemy
- **Quality check** : vérification des doublons et des valeurs aberrantes sur les données fraîchement insérées
### 5. Gestion des erreurs
 
Chaque tâche a 3 retries avec 5 minutes d'attente entre chaque tentative. Les erreurs par ville dans l'extract sont loggées sans faire échouer tout le run — sauf si aucune ville ne répond, auquel cas une exception est levée.
 
## Arrêter la stack
 
```bash
docker-compose down
```

---

