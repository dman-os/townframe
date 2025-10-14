use crate::interlude::*;

mod version_updates;

/// Initialize the automerge document based on globals, and start connector lazily.
pub async fn init_from_globals(cx: &Ctx) -> Res<()> {
    // Try to recover existing doc_id from local globals kv
    let init_state = crate::globals::get_init_state(cx).await?;
    let (handle_app, handle_drarwer) = if let crate::globals::InitState::Created {
        doc_id_app,
        doc_id_drawer,
    } = init_state
    {
        let (handle_app, handle_drawer) =
            tokio::try_join!(cx.acx.find_doc(doc_id_app), cx.acx.find_doc(doc_id_drawer))?;
        if handle_app.is_none() {
            warn!("doc not found locally for stored doc_id_app; creating new local document");
        }
        if handle_drawer.is_none() {
            warn!("doc not found locally for stored doc_id_drawer; creating new local document");
        }
        (handle_app, handle_drawer)
    } else {
        default()
    };
    let mut doc_handles = vec![];
    let mut update_state = false;
    for (handle, latest_fn) in [
        (
            handle_app,
            version_updates::app::version_latest as fn() -> Res<Vec<u8>>,
        ),
        (handle_drarwer, version_updates::drawer::version_latest),
    ] {
        let handle = match handle {
            Some(handle) => handle,
            None => {
                update_state = true;
                let doc = latest_fn()?;
                let doc =
                    automerge::Automerge::load(&doc).wrap_err("error loading version_latest")?;
                let handle = cx.acx.add_doc(doc).await?;
                handle
            }
        };
        doc_handles.push(handle)
    }
    if doc_handles.len() != 2 {
        unreachable!();
    }
    for handle in &doc_handles {
        cx.acx
            .change_manager()
            .clone()
            .spawn_doc_listener(handle.clone());
    }
    if update_state {
        crate::globals::set_init_state(
            cx,
            &crate::globals::InitState::Created {
                doc_id_app: doc_handles[0].document_id().clone(),
                doc_id_drawer: doc_handles[1].document_id().clone(),
            },
        )
        .await?;
    }
    let (Ok(()), Ok(())) = (
        cx.doc_drawer.set(doc_handles.pop().unwrap_or_log()),
        cx.doc_app.set(doc_handles.pop().unwrap_or_log()),
    ) else {
        eyre::bail!("double ctx initialization");
    };
    Ok(())
}
