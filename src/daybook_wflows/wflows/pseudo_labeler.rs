use crate::interlude::*;
use wflow_sdk::{JobErrorX, Json, WflowCtx};

pub fn run(cx: WflowCtx) -> Result<(), JobErrorX> {
    use crate::wit::townframe::daybook::drawer;
    use crate::wit::townframe::daybook::facet_routine;

    let args = facet_routine::get_args();

    // Find the working facet token (the one with write access matching facet_key)
    let working_facet_token = args
        .rw_facet_tokens
        .iter()
        .find(|(key, _)| key == &args.facet_key)
        .map(|(_, token)| token)
        .ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "working facet key '{}' not found in rw_facet_tokens",
                args.facet_key
            ))
        })?;

    // Get doc using drawer interface
    let doc = drawer::get_doc_at_heads(&args.doc_id, &args.heads)
        .map_err(|err| JobErrorX::Terminal(ferr!("error getting doc: {err:?}")))?;

    // Extract text content for LLM
    // Use root types since Doc uses root types (not WIT types)
    use daybook_types::doc::{Note, WellKnownFacet, WellKnownFacetTag};
    let content_text = match doc
        .facets
        .iter()
        .find(|(facet_key, _)| {
            let facet_key = daybook_types::doc::FacetKey::from(facet_key.as_str());
            facet_key.tag == daybook_types::doc::FacetTag::WellKnown(WellKnownFacetTag::Note)
        })
        .map(|(_, val)| {
            let raw = serde_json::from_str(val).map_err(|err| {
                JobErrorX::Terminal(ferr!("unable to parse facet found on doc: {err}"))
            })?;
            WellKnownFacet::from_json(raw, WellKnownFacetTag::Note).map_err(|err| {
                JobErrorX::Terminal(err.wrap_err("unable to parse facet found on doc"))
            })
        }) {
        Some(Ok(WellKnownFacet::Note(Note { content, .. }))) => content,
        Some(Ok(_)) => unreachable!(),
        Some(Err(err)) => return Err(err),
        None => {
            return Err(JobErrorX::Terminal(ferr!(
                "no {tag} found on doc",
                tag = WellKnownFacetTag::Note.as_str()
            )))
        }
    };

    const MAX_LLM_INPUT_CHARS: usize = 8_000;
    const MAX_LABEL_CHARS: usize = 64;
    let content_text_for_prompt = if content_text.chars().count() > MAX_LLM_INPUT_CHARS {
        let mut truncated = content_text
            .chars()
            .take(MAX_LLM_INPUT_CHARS)
            .collect::<String>();
        truncated.push_str("\n[truncated]");
        truncated
    } else {
        content_text.clone()
    };

    // Call the LLM to generate a label
    let llm_response: String = cx.effect(|| {
        use crate::wit::townframe::daybook::mltools_llm_chat;

        let message_text = format!(
            "Based on the following document content, provide a single short label or category (1-3 words). \
            Just return the label, nothing else.\n\nDocument content:\n{}",
            content_text_for_prompt
        );
        let result = mltools_llm_chat::llm_chat(&message_text);

        match result {
            Ok(response_text) => {
                // Clean up the response - remove quotes, trim whitespace
                let label = response_text
                    .trim()
                    .trim_matches('"')
                    .trim_matches('\'')
                    .trim()
                    .to_string();
                let label = label.split_whitespace().collect::<Vec<_>>().join(" ");
                let word_count = label.split_whitespace().count();
                if label.is_empty() {
                    return Err(JobErrorX::Terminal(ferr!(
                        "llm returned an empty label after normalization"
                    )));
                }
                if !(1..=3).contains(&word_count) {
                    return Err(JobErrorX::Terminal(ferr!(
                        "llm returned invalid label word count {word_count}; expected 1..=3"
                    )));
                }
                if label.chars().count() > MAX_LABEL_CHARS {
                    return Err(JobErrorX::Terminal(ferr!(
                        "llm returned label longer than {MAX_LABEL_CHARS} chars"
                    )));
                }
                Ok(Json(label))
            }
            Err(err) => Err(JobErrorX::Terminal(ferr!("error calling LLM: {err}"))),
        }
    })?;

    let new_labels = vec![llm_response];

    cx.effect(|| {
        let new_facet: daybook_types::doc::FacetRaw =
            WellKnownFacet::PseudoLabel(new_labels).into();
        let new_facet = serde_json::to_string(&new_facet).expect(ERROR_JSON);
        working_facet_token
            .update(&new_facet)
            .wrap_err("error updating facet")
            .map_err(JobErrorX::Terminal)?;
        Ok(Json(()))
    })?;

    Ok(())
}
