CREATE TABLE IF NOT EXISTS cabinet_domain_grants (
    namespace TEXT NOT NULL
  , subject TEXT NOT NULL
  , object TEXT NOT NULL
  , rights INTEGER NOT NULL
  , PRIMARY KEY (namespace, subject, object)
);
