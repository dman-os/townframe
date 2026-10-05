SELECT backend_id
     , path
     , kind
     , source_backend_id
     , source_output
     , source_version
     , size
     , target
  FROM pauperfuse_observed_entry
 ORDER BY backend_id
        , path
 LIMIT ?;
