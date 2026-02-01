# ============================================================
# Financial CDC Pipeline – PostgreSQL → Debezium → Kafka
# ============================================================

# Objectif du projet
# ------------------
# Mettre en place une pipeline professionnelle de Change Data Capture (CDC) :
# - Génération de données financières fictives (Python + Faker)
# - Stockage dans PostgreSQL
# - Capture automatique des changements via Debezium
# - Diffusion temps réel dans Kafka
# - Visualisation via Debezium UI et Control Center
# Le tout orchestré avec Docker Compose.


# ============================================================
# Architecture
# ============================================================
#
# Python (Faker)
#      |
#      v
# PostgreSQL (financialDB)
#      |
#      v  (WAL logical)
# Debezium Connector
#      |
#      v
# Kafka Broker
#      |
#      v
# Consumers / Dashboards / Analytics
#


# ============================================================
# Technologies utilisées
# ============================================================
# PostgreSQL      : base de données source
# Debezium        : Change Data Capture
# Kafka           : streaming temps réel
# Zookeeper       : coordination Kafka
# Control Center  : monitoring Kafka
# Debezium UI     : gestion des connectors
# Python + Faker  : génération de données
# Docker Compose  : orchestration


# ============================================================
# Lancement de l'infrastructure
# ============================================================

docker compose up -d

# Interfaces :
# Kafka Control Center : http://localhost:9021
# Debezium UI         : http://localhost:8080


# ============================================================
# Génération des données
# ============================================================

pip install psycopg2-binary faker
python main.py

# Le script :
# - crée la table transactions
# - génère une transaction aléatoire
# - l’insère dans PostgreSQL


# ============================================================
# Configuration PostgreSQL pour CDC
# ============================================================

psql -U postgres -d financialDB -c "ALTER TABLE transactions REPLICA IDENTITY FULL;"

# Signification :
# Pour chaque UPDATE ou DELETE,
# PostgreSQL envoie la ligne complète dans le WAL.
# Indispensable pour Debezium.


# ============================================================
# Problème DECIMAL Debezium
# ============================================================

# Par défaut (mode precise) :
# "amount": { "scale": 2, "value": "AAAB9A==" }
#
# Cause :
# Debezium encode les DECIMAL en binaire (base64)
# pour préserver une précision mathématique parfaite.
#
# Problème :
# Inutilisable pour dashboards, analytics, debug humain.


# ============================================================
# Solution recommandée (Data Engineering)
# ============================================================

curl -X PUT http://localhost:8093/connectors/postgres_connector/config \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "topic.prefix": "cdc",
    "database.user": "postgres",
    "database.password": "amal",
    "database.dbname": "financialDB",
    "database.hostname": "postgres",
    "database.port": "5432",
    "plugin.name": "pgoutput",
    "decimal.handling.mode": "double"
  }'

curl -X POST http://localhost:8093/connectors/postgres_connector/restart

# Résultat :
# Avant : "value": "AAAB9A=="
# Après : "amount": 459.38


# ============================================================
# Trigger 1 – Traçabilité utilisateur
# ============================================================

psql -U postgres -d financialDB <<'SQL'
CREATE OR REPLACE FUNCTION record_change_user()
RETURNS trigger AS $$
BEGIN
  NEW.modified_by := current_user;
  NEW.modified_at := current_timestamp;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_user_update
BEFORE UPDATE ON transactions
FOR EACH ROW
EXECUTE FUNCTION record_change_user();
SQL

# Ce trigger ajoute automatiquement :
# - qui a modifié
# - quand


# ============================================================
# Trigger 2 – Journal JSON des colonnes modifiées
# ============================================================

psql -U postgres -d financialDB <<'SQL'
ALTER TABLE transactions ADD change_info JSONB;

CREATE OR REPLACE FUNCTION record_changed_columns()
RETURNS trigger AS $$
DECLARE
  change_details JSONB;
BEGIN
  change_details := '{}'::JSONB;

  IF NEW.amount IS DISTINCT FROM OLD.amount THEN
    change_details := jsonb_insert(
      change_details,
      '{amount}',
      jsonb_build_object('old', OLD.amount, 'new', NEW.amount)
    );
  END IF;

  change_details := change_details ||
    jsonb_build_object(
      'modified_by', current_user,
      'modified_at', now()
    );

  NEW.change_info := change_details;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_record_change
BEFORE UPDATE ON transactions
FOR EACH ROW
EXECUTE FUNCTION record_changed_columns();
SQL


# ============================================================
# Étapes pédagogiques : INSERT & Traçabilité
# ============================================================

# Étape 1 : INSERT simple
psql -U postgres -d financialDB <<'SQL'
INSERT INTO transactions (
  transaction_id,
  user_id,
  timestamp,
  amount,
  currency,
  city,
  country,
  merchant_name,
  payment_method,
  ip_address,
  voucher_code,
  affiliate_id
)
VALUES (
  'tx_001',
  'amal',
  now(),
  200,
  'EUR',
  'Paris',
  'France',
  'Amazon',
  'credit_card',
  '192.168.1.1',
  '',
  'aff_001'
);
SQL

# Étape 2 : UPDATE
psql -U postgres -d financialDB <<'SQL'
UPDATE transactions
SET amount = 800
WHERE transaction_id = 'tx_001';
SQL

# Étape 3 : Vérification
psql -U postgres -d financialDB <<'SQL'
SELECT
  transaction_id,
  amount,
  modified_by,
  modified_at,
  change_info
FROM transactions
WHERE transaction_id = 'tx_001';
SQL


# Exemple change_info réel
# ------------------------
# {
#   "amount": { "old": 200, "new": 800 },
#   "modified_by": "postgres",
#   "modified_at": "2026-02-01T18:42:31"
# }


# ============================================================
# Mental model (à retenir absolument)
# ============================================================
# Application → SQL simple
# Base        → intelligence métier
#
# On ne fait PAS confiance au code applicatif
# On fait confiance au moteur PostgreSQL.
#
# La base devient responsable de :
# - la vérité
# - l’audit
# - la conformité
# - la traçabilité


# ============================================================
# Règle mentale Debezium
# ============================================================
# Nouveau connector     → POST /connectors
# Modifier config       → PUT /connectors/{name}/config
# Appliquer changement  → PUT + restart


# ============================================================
# Les 3 modes DECIMAL Debezium
# ============================================================
# precise : bytes/base64 → finance stricte
# string  : "459.38"     → JSON simple
# double  : 459.38       → analytics / streaming


# ============================================================
# Cas d’usage réel
# ============================================================
# - Anti-fraude bancaire
# - Monitoring paiements
# - Realtime dashboards
# - Data Lake ingestion
# - Audit & conformité
# - Event-driven systems


# ============================================================
# Conclusion
# ============================================================
# Ce projet implémente une architecture CDC professionnelle :
# - PostgreSQL correctement configuré
# - Debezium maîtrisé
# - Kafka opérationnel
# - Triggers SQL robustes
# - Journal JSON automatique
# - Orchestration Docker
#
# Exactement le type de pipeline utilisée
# en Data Engineering réel en entreprise.
