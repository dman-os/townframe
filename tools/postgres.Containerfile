FROM docker.io/library/postgres:17-bookworm

RUN set -eux; \
  apt-get update; \
  apt-get install -y --no-install-recommends curl ca-certificates libc6 libcurl4 gnupg lsb-release; \
  mkdir -p /etc/apt/keyrings; \
  curl -fsSL https://repo.pigsty.cc/key | gpg --dearmor -o /etc/apt/keyrings/pigsty.gpg; \
  CODENAME=$(lsb_release -cs); \
  echo "deb [signed-by=/etc/apt/keyrings/pigsty.gpg] https://repo.pigsty.io/apt/infra generic main" > /etc/apt/sources.list.d/pigsty-io.list; \
  echo "deb [signed-by=/etc/apt/keyrings/pigsty.gpg] https://repo.pigsty.io/apt/pgsql/${CODENAME} ${CODENAME} main" >> /etc/apt/sources.list.d/pigsty-io.list; \
  apt-get update; \
  apt-get install -y pig;

RUN pig repo add pgsql -ru \
  && pig ext install -y pg_http pg_jsonschema pgtap pg_uuidv7 \
  && pig ext status;
