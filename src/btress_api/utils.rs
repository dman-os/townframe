pub mod testing {
    use crate::interlude::*;

    use api_utils_rs::testing::*;

    pub async fn state_fn_service(testing: &TestContext) -> Res<crate::SharedServiceContext> {
        Ok(crate::SharedServiceContext(crate::ServiceContext(
            state_fn(testing).await?,
        )))
    }

    pub async fn cx_fn_service(
        test_name: &'static str,
    ) -> Res<(TestContext, crate::SharedServiceContext)> {
        let btress_db = test_db(test_name).await;
        let testing = TestContext::new(test_name.into(), [("btress".to_string(), btress_db)], []);
        let cx = state_fn_service(&testing).await?;
        Ok((testing, cx))
    }

    pub async fn test_db(test_name: &'static str) -> TestPg {
        utils_rs::testing::load_envs_once();
        let db_name = test_name.replace("::tests::", "_").replace("::", "_");
        TestPg::new(
            db_name,
            std::path::Path::new(&utils_rs::get_env_var("BTRESS_API_ROOT_PATH").unwrap()),
        )
        .await
    }

    pub async fn state_fn(
        // db_pool: sqlx::postgres::PgPool,
        // epigram_cx: epigram_api::SharedContext,
        testing: &TestContext,
    ) -> Res<crate::SharedContext> {
        let kanidm = kanidm_client::KanidmClientBuilder::new()
            .address("https://localhost:8443".into())
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true)
            .build()
            .map_err(|err| ferr!("{err:?}"))?;
        {
            let pass = std::env::var("KANIDM_TFRAME_ADMIN_PASS").expect(
                "env KANIDM_TFRAME_ADMIN_PASS required, make sure to run ghjk x kanidm-recover",
            );
            kanidm
                .auth_simple_password("tframe_admin", &pass)
                .await
                .map_err(|err| ferr!("{err:?}"))
                .wrap_err("error authenticating kanidm")?;
        }
        let ctx = crate::Context {
            db: api::StdDb::Pg {
                db_pool: testing.pg_pools["btress"].pool.clone(),
            },
            config: crate::Config {
                pass_salt_hash: Arc::new(argon2::password_hash::SaltString::generate(
                    &mut argon2::password_hash::rand_core::OsRng,
                )),
            },
            argon2: Arc::new(argon2::Argon2::default()),
            kanidm,
        };
        Ok(Arc::new(ctx))
    }

    pub async fn cx_fn(test_name: &'static str) -> Res<(TestContext, crate::SharedContext)> {
        let testing = TestContext::new(
            test_name.into(),
            [("btress".to_string(), test_db(test_name).await)],
            [],
        );
        let cx = state_fn(&testing).await?;
        Ok((testing, cx))
    }
}
