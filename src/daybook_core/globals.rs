use crate::interlude::*;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub enum InitState {
    None,
    Created { doc_id: samod::DocumentId },
}

const INIT_STATE_KEY: &str = "init_state";

pub async fn get_init_state(cx: &Ctx) -> Res<InitState> {
    let val = super::sql::kv::get(cx, INIT_STATE_KEY).await?;
    let state = match val {
        Some(json) => serde_json::from_str::<InitState>(&json)?,
        None => InitState::None,
    };
    Ok(state)
}

pub async fn set_init_state(cx: &Ctx, state: &InitState) -> Res<()> {
    let json = serde_json::to_string(state)?;
    super::sql::kv::set(cx, INIT_STATE_KEY, &json).await
}
