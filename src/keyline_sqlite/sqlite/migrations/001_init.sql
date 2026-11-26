-- The Keyline certificate set: signed delegations and revocations, and the set's version.

-- One row: the version of the set. It moves by one in every transaction that adds a certificate,
-- inside that transaction, so a rollback leaves it where it was and every snapshot reads it
-- together with the rows it describes.
CREATE TABLE keyline_meta (
      id INTEGER PRIMARY KEY CHECK (id = 1)
    , version INTEGER NOT NULL CHECK (version >= 0)
) STRICT;

INSERT INTO keyline_meta (id, version) VALUES (1, 0);

-- Every certificate of the set, delegations and revocations in one table keyed by identity.
-- `kind` is 0 for a delegation and 1 for a revocation, and `(kind, digest)` is `keyline`'s
-- `CertificateId` order, so a scan of the table is the `certificates` listing. The two kinds'
-- digests are hashed under different kinds and never collide; `kind` is in the key to order
-- them, not to tell them apart.
--
-- `signed` is the certificate as received (the wire frame of its signed form) and is the source
-- of truth; the other columns are the fields the queries and indexes need, copied out of it by
-- the same insert, never read back to rebuild a certificate.
CREATE TABLE keyline_certificates (
      kind INTEGER NOT NULL CHECK (kind IN (0, 1))
    , digest BLOB NOT NULL CHECK (length(digest) = 32)
    , issuer BLOB NOT NULL CHECK (length(issuer) = 32)
      -- delegations: who the grant is to, what it is about, and its level as a rank
      -- (0 Relay, 1 Read, 2 Edit, 3 Admin: the lattice order, not the wire tag), and the
      -- revocation it is re-issued past
    , audience BLOB CHECK (length(audience) = 32)
    , subject BLOB CHECK (length(subject) = 32)
    , power INTEGER CHECK (power BETWEEN 0 AND 3)
    , citation BLOB CHECK (length(citation) = 32)
      -- revocations: the delegation withdrawn, and the retention watermarks it carries
      -- (`subject (32) || length (u32 big endian) || watermark`, repeated, in subject order;
      -- NULL for none). Evaluation never reads them.
    , revoke BLOB CHECK (length(revoke) = 32)
    , retain BLOB
    , signed BLOB NOT NULL
    , PRIMARY KEY (kind, digest)
    , CHECK (
          (kind = 0 AND audience IS NOT NULL AND subject IS NOT NULL AND power IS NOT NULL
               AND revoke IS NULL AND retain IS NULL)
       OR (kind = 1 AND revoke IS NOT NULL
               AND audience IS NULL AND subject IS NULL AND power IS NULL AND citation IS NULL)
      )
) STRICT, WITHOUT ROWID;

-- `delegations_to`: the delegations about one subject to one audience.
CREATE INDEX keyline_delegations_by_subject_audience
    ON keyline_certificates (subject, audience)
 WHERE kind = 0;

-- `revocations_of`, and the evaluation's question of who revoked a delegation: the revocations
-- naming one delegation, with their issuers.
CREATE INDEX keyline_revocations_by_target
    ON keyline_certificates (revoke, issuer)
 WHERE kind = 1;
