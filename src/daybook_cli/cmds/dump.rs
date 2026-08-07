use crate::interlude::*;

pub async fn run() -> Res<ExitCode> {
    let cx = lazy::repo_ctx().await?;
    let mut drawer = cx
        .doc_drawer
        .with_document(|doc| {
            let value: ThroughJson<serde_json::Value> = autosurgeon::hydrate(doc)?;
            eyre::Ok(value.0)
        })
        .await??;
    let mut app = cx
        .doc_app
        .with_document(|doc| {
            let value: ThroughJson<serde_json::Value> = autosurgeon::hydrate(doc)?;
            eyre::Ok(value.0)
        })
        .await??;
    fn display_byte_array(val: &mut serde_json::Value) {
        match val {
            serde_json::Value::Array(values) => {
                if values
                    .iter()
                    .all(|val| matches!(val, serde_json::Value::Number(..)))
                {
                    *val = serde_json::Value::String(format!("byte array, len = {}", values.len()))
                } else {
                    for val in values {
                        display_byte_array(val);
                    }
                }
            }
            serde_json::Value::Object(map) => {
                for (_, val) in map {
                    display_byte_array(val)
                }
            }
            _ => {}
        }
    }
    display_byte_array(&mut drawer);
    display_byte_array(&mut app);
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "drawer": drawer,
            "app": app,
        }))?
    );
    Ok(ExitCode::SUCCESS)
}
