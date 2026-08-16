use crate::db::learned_rules::RuleFingerprint;
use crate::error::AhError;
use crate::provider::extensions::ExtensionConfig;
use serde::Deserialize;
use serde_json::Value;

pub(super) fn required_str<'a>(params: &'a Value, field: &str) -> Result<&'a str, AhError> {
    params
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| AhError::IpcInvalidRequest(format!("missing or invalid field '{field}'")))
}

pub(super) fn required_i64(params: &Value, field: &str) -> Result<i64, AhError> {
    params
        .get(field)
        .and_then(Value::as_i64)
        .ok_or_else(|| AhError::IpcInvalidRequest(format!("missing or invalid field '{field}'")))
}

pub(super) fn extension_config_from_params(params: &Value) -> Result<ExtensionConfig, AhError> {
    Ok(ExtensionConfig {
        hooks: match params.get("hooks") {
            Some(value) => serde_json::from_value(value.clone())
                .map_err(|err| AhError::IpcInvalidRequest(format!("invalid hooks: {err}")))?,
            None => Default::default(),
        },
        plugins: match params.get("plugins") {
            Some(value) => serde_json::from_value(value.clone())
                .map_err(|err| AhError::IpcInvalidRequest(format!("invalid plugins: {err}")))?,
            None => Default::default(),
        },
        skills: match params.get("skills") {
            Some(value) => serde_json::from_value(value.clone())
                .map_err(|err| AhError::IpcInvalidRequest(format!("invalid skills: {err}")))?,
            None => Default::default(),
        },
        bundle: match params.get("bundle") {
            Some(value) => parse_bundle_refs(value.clone())?,
            None => Default::default(),
        },
        settings: match params.get("settings") {
            Some(value) => serde_json::from_value(value.clone())
                .map_err(|err| AhError::IpcInvalidRequest(format!("invalid settings: {err}")))?,
            None => Default::default(),
        },
        mcp: match params.get("mcp") {
            Some(value) => serde_json::from_value(value.clone())
                .map_err(|err| AhError::IpcInvalidRequest(format!("invalid mcp: {err}")))?,
            None => Default::default(),
        },
        rules: Default::default(),
        bundle_digest: Default::default(),
        resolved_skills: Default::default(),
    })
}

fn parse_bundle_refs(value: Value) -> Result<Vec<String>, AhError> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum BundleInput {
        Single(String),
        Many(Vec<String>),
    }

    match serde_json::from_value(value)
        .map_err(|err| AhError::IpcInvalidRequest(format!("invalid bundle: {err}")))?
    {
        BundleInput::Single(value) => Ok(vec![value]),
        BundleInput::Many(values) => Ok(values),
    }
}

pub(super) fn optional_bool(params: &Value, field: &str, default: bool) -> Result<bool, AhError> {
    match params.get(field) {
        Some(value) => value
            .as_bool()
            .ok_or_else(|| AhError::IpcInvalidRequest(format!("invalid field '{field}'"))),
        None => Ok(default),
    }
}

pub(super) fn parse_rule_fingerprint(params: &Value) -> Result<RuleFingerprint, AhError> {
    let fingerprint = params
        .get("fingerprint")
        .ok_or_else(|| AhError::IpcInvalidRequest("missing field 'fingerprint'".to_string()))?;
    let fingerprint_type = fingerprint
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            AhError::IpcInvalidRequest("missing or invalid field 'fingerprint.type'".to_string())
        })?;
    match fingerprint_type {
        "regex" => {
            let pattern = fingerprint
                .get("pattern")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    AhError::IpcInvalidRequest(
                        "missing or invalid field 'fingerprint.pattern'".to_string(),
                    )
                })?;
            Ok(RuleFingerprint::Regex {
                pattern: pattern.to_string(),
            })
        }
        _ => Err(AhError::IpcInvalidRequest(format!(
            "unsupported fingerprint.type: {fingerprint_type}"
        ))),
    }
}

pub(super) fn required_string_array(params: &Value, field: &str) -> Result<Vec<String>, AhError> {
    let values = optional_string_array(params, field)?
        .ok_or_else(|| AhError::IpcInvalidRequest(format!("missing field '{field}'")))?;
    if values.is_empty() {
        return Err(AhError::IpcInvalidRequest(format!(
            "{field} must contain at least one positive example"
        )));
    }
    if values.len() > 10 {
        return Err(AhError::IpcInvalidRequest(format!(
            "{field} must contain at most 10 examples"
        )));
    }
    if values.iter().any(|value| value.len() > 16 * 1024) {
        return Err(AhError::IpcInvalidRequest(format!(
            "{field} examples must be at most 16 KiB each"
        )));
    }
    Ok(values)
}

pub(super) fn optional_string_array(
    params: &Value,
    field: &str,
) -> Result<Option<Vec<String>>, AhError> {
    let Some(value) = params.get(field) else {
        return Ok(None);
    };
    let array = value
        .as_array()
        .ok_or_else(|| AhError::IpcInvalidRequest(format!("invalid field '{field}'")))?;
    array
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| AhError::IpcInvalidRequest(format!("invalid field '{field}'")))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

pub(super) fn optional_json_field<T: for<'de> Deserialize<'de>>(
    params: &Value,
    field: &str,
) -> Result<Option<T>, AhError> {
    match params.get(field) {
        Some(Value::Null) | None => Ok(None),
        Some(value) => serde_json::from_value(value.clone())
            .map(Some)
            .map_err(|err| AhError::IpcInvalidRequest(format!("invalid field '{field}': {err}"))),
    }
}
