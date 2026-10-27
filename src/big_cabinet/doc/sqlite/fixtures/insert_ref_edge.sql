INSERT INTO big_cabinet_facet_ref_edges
     ( origin_doc
     , origin_facet
     , origin_path
     , target_doc
     , target_facet
     , ref_heads
     )
VALUES ( :origin_doc
       , :facet
       , '/link'
       , :target_doc
       , :facet
       , :ref_heads
       )
ON CONFLICT DO NOTHING;
