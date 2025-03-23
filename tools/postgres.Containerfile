FROM docker.io/library/rust:1.84.1 as trunk-build

ENV PATH "$PATH:/root/.cargo/bin/"

RUN set -eux \
  && cargo install pg-trunk --locked --debug

FROM docker.io/library/postgres:17 as final

RUN set -eux \
  && apt update \
  # some of this are required by the extensions \
  && apt install -y ca-certificates libc6 libcurl4


COPY --from=trunk-build /usr/local/cargo/bin/trunk /bin/trunk

RUN trunk install pgsql_http \
  && trunk install pg_jsonschema \
  && trunk install pgtap \
  && trunk install pg_uuidv7

# RUN set -eux \
#   && apt update \
#   && apt install -y curl sudo gnupg lsb-release \
#   && bash <<'EOS'
# curl -fsSL https://repo.pigsty.io/key | sudo gpg --dearmor -o /etc/apt/keyrings/pigsty.gpg  # add gpg key
# sudo tee /etc/apt/sources.list.d/pigsty-io.list > /dev/null <<EOF
# deb [signed-by=/etc/apt/keyrings/pigsty.gpg] https://repo.pigsty.io/apt/infra generic main 
# deb [signed-by=/etc/apt/keyrings/pigsty.gpg] https://repo.pigsty.io/apt/pgsql/$(lsb_release -cs) $(lsb_release -cs) main
# EOF
# sudo apt update;  
# sudo apt install -y pig
# EOS
#
# RUN set -eux \
#   && pig repo add pigsty -ru  # add pgdg & pigsty repo, update cache \
#   && pig ext install pg_http pg_jsonschema pgtap pg_uuidv7 \
#   && pig ext status
#
