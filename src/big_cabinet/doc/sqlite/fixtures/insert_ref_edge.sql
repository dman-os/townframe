INSERT INTO big_cabinet_datum_ref_edges
     ( origin_doc
     , origin_datum
     , origin_path
     , target_doc
     , target_datum
     , ref_heads
     )
VALUES ( :origin_doc
       , :datum
       , '/link'
       , :target_doc
       , :datum
       , :ref_heads
       )
ON CONFLICT DO NOTHING;
