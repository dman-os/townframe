// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/createFileHeaderElement.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `FileDiffMetadata | FileContents` is `FileOrDiff` (upstream distinguishes them by `'type' in fileOrDiff`).

use crate::constants::CUSTOM_HEADER_SLOT_ID;
use crate::constants::HEADER_FILENAME_SUFFIX_SLOT_ID;
use crate::constants::HEADER_METADATA_SLOT_ID;
use crate::constants::HEADER_PREFIX_SLOT_ID;
use crate::hast::Element;
use crate::hast::ElementContent;
use crate::hast::PropValue;
use crate::hast::Properties;
use crate::sprite::SvgSpriteName;
use crate::types::options::FileHeaderRenderMode;
use crate::types::patch::ChangeTypes;
use crate::types::patch::FileContents;
use crate::types::patch::FileDiffMetadata;
use crate::utils::render::get_icon_for_type::IconType;
use crate::utils::render::get_icon_for_type::get_icon_for_type;
use crate::utils::render::hast_utils::create_hast_element;
use crate::utils::render::hast_utils::create_icon_element;
use crate::utils::render::hast_utils::create_text_node_element;

/// `FileDiffMetadata | FileContents`
#[derive(Debug, Clone, Copy)]
pub enum FileOrDiff<'file> {
    Diff(&'file FileDiffMetadata),
    File(&'file FileContents),
}

#[derive(Debug, Clone, Copy)]
pub struct CreateFileHeaderElementProps<'file> {
    pub file_or_diff: FileOrDiff<'file>,
    pub mode: FileHeaderRenderMode,
    pub sticky_header: bool,
}

fn change_type_name(change_type: ChangeTypes) -> &'static str {
    match change_type {
        ChangeTypes::Change => "change",
        ChangeTypes::RenamePure => "rename-pure",
        ChangeTypes::RenameChanged => "rename-changed",
        ChangeTypes::New => "new",
        ChangeTypes::Deleted => "deleted",
    }
}

pub fn create_file_header_element(props: CreateFileHeaderElementProps<'_>) -> Element {
    let CreateFileHeaderElementProps {
        file_or_diff,
        mode,
        sticky_header,
    } = props;
    let file_diff = match file_or_diff {
        FileOrDiff::Diff(diff) => Some(diff),
        FileOrDiff::File(_) => None,
    };
    let mode_name = match mode {
        FileHeaderRenderMode::Default => "default",
        FileHeaderRenderMode::Custom => "custom",
    };
    let properties = Properties::new()
        .with("data-diffs-header", mode_name)
        .with(
            "data-change-type",
            file_diff.map(|diff| change_type_name(diff.change_type)),
        )
        .with(
            "data-sticky",
            if sticky_header {
                PropValue::Str(String::new())
            } else {
                PropValue::Undefined
            },
        );

    let (name, prev_name) = match file_or_diff {
        FileOrDiff::Diff(diff) => (diff.name.as_str(), diff.prev_name.as_deref()),
        FileOrDiff::File(file) => (file.name.as_str(), None),
    };
    let mut children: Vec<ElementContent> = Vec::new();
    if mode == FileHeaderRenderMode::Custom {
        children.push(
            create_hast_element(
                "slot",
                Vec::new(),
                Properties::new().with("name", CUSTOM_HEADER_SLOT_ID),
            )
            .into(),
        );
    } else {
        children.push(
            create_header_element(
                name,
                prev_name,
                file_diff.map_or(IconType::File, |diff| IconType::Change(diff.change_type)),
            )
            .into(),
        );
        children.push(create_metadata_element(file_diff).into());
    }
    create_hast_element("div", children, properties)
}

fn create_header_element(name: &str, prev_name: Option<&str>, icon_type: IconType) -> Element {
    let icon_type_name = match icon_type {
        IconType::File => "file",
        IconType::Change(change_type) => change_type_name(change_type),
    };
    let mut children: Vec<ElementContent> = vec![
        create_hast_element(
            "slot",
            Vec::new(),
            Properties::new().with("name", HEADER_PREFIX_SLOT_ID),
        )
        .into(),
        create_icon_element(
            get_icon_for_type(icon_type),
            16,
            16,
            &Properties::new().with("data-change-icon", icon_type_name),
        )
        .into(),
    ];
    if let Some(prev_name) = prev_name {
        children.push(
            create_hast_element(
                "div",
                vec![
                    create_hast_element(
                        "bdi",
                        vec![create_text_node_element(prev_name).into()],
                        Properties::new(),
                    )
                    .into(),
                ],
                Properties::new().with("data-prev-name", ""),
            )
            .into(),
        );
        children.push(
            create_icon_element(
                SvgSpriteName::ArrowRightShort,
                16,
                16,
                &Properties::new().with("data-rename-icon", ""),
            )
            .into(),
        );
    }
    children.push(
        create_hast_element(
            "div",
            vec![
                create_hast_element(
                    "bdi",
                    vec![create_text_node_element(name).into()],
                    Properties::new(),
                )
                .into(),
            ],
            Properties::new().with("data-title", ""),
        )
        .into(),
    );
    children.push(
        create_hast_element(
            "slot",
            Vec::new(),
            Properties::new().with("name", HEADER_FILENAME_SUFFIX_SLOT_ID),
        )
        .into(),
    );
    create_hast_element(
        "div",
        children,
        Properties::new().with("data-header-content", ""),
    )
}

fn create_metadata_element(file_diff: Option<&FileDiffMetadata>) -> Element {
    let mut children: Vec<ElementContent> = Vec::new();
    if let Some(file_diff) = file_diff {
        let additions: usize = file_diff.hunks.iter().map(|hunk| hunk.addition_lines).sum();
        let deletions: usize = file_diff.hunks.iter().map(|hunk| hunk.deletion_lines).sum();
        if deletions > 0 || additions == 0 {
            children.push(
                create_hast_element(
                    "span",
                    vec![create_text_node_element(&format!("-{deletions}")).into()],
                    Properties::new().with("data-deletions-count", ""),
                )
                .into(),
            );
        }
        if additions > 0 || deletions == 0 {
            children.push(
                create_hast_element(
                    "span",
                    vec![create_text_node_element(&format!("+{additions}")).into()],
                    Properties::new().with("data-additions-count", ""),
                )
                .into(),
            );
        }
    }
    children.push(
        create_hast_element(
            "slot",
            Vec::new(),
            Properties::new().with("name", HEADER_METADATA_SLOT_ID),
        )
        .into(),
    );
    create_hast_element("div", children, Properties::new().with("data-metadata", ""))
}
