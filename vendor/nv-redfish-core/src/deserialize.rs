// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use serde::Deserialize;
use serde::Deserializer;

/// Deserialize an optional nullable field. nv-redfish models these fields
/// with `Option<Option<T>>`, where `None` means "no field" and
/// `Some(None)` means the field is explicitly set to null.
///
/// # Errors
///
/// Returns an error if deserialization of the underlying type fails.
pub fn de_optional_nullable<'de, D, T>(de: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Deserialize::deserialize(de).map(Some)
}

/// Deserialize a required nullable field. nv-redfish models these fields
/// with `Option<T>`, where `None` means null.
///
/// # Errors
///
/// Returns an error if deserialization of the underlying type fails.
pub fn de_required_nullable<'de, D, T>(de: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Deserialize::deserialize(de)
}

/// Deserialize a required, non-nullable array — nv-redfish models these
/// fields with `Vec<T>`. An empty array is spelled `[]` in Redfish, but some
/// service implementations send `null` instead (the BlueField-2 BMC 24.10
/// `BootOptions` collection sends `"Members": null` alongside
/// `"Members@odata.count": 0`); treat `null` as an empty array rather than
/// failing the whole resource.
///
/// # Errors
///
/// Returns an error if deserialization of the underlying type fails.
pub fn de_required_collection<'de, D, T>(de: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<Vec<T>>::deserialize(de).map(Option::unwrap_or_default)
}

#[cfg(test)]
mod tests {
    use super::de_required_collection;
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct Collection {
        #[serde(rename = "Members", deserialize_with = "de_required_collection")]
        members: Vec<String>,
        #[serde(rename = "Members@odata.count")]
        count: u32,
    }

    #[test]
    fn test_required_collection_null_is_empty() {
        let json = r#"{"Members": null, "Members@odata.count": 0}"#;
        let collection: Collection =
            serde_json::from_str(json).expect("null Members must deserialize as an empty array");
        assert!(collection.members.is_empty());
        assert_eq!(collection.count, 0);
    }

    #[test]
    fn test_required_collection_empty_array() {
        let json = r#"{"Members": [], "Members@odata.count": 0}"#;
        let collection: Collection = serde_json::from_str(json).expect("empty Members");
        assert!(collection.members.is_empty());
    }

    #[test]
    fn test_required_collection_populated() {
        let json = r#"{"Members": ["a", "b"], "Members@odata.count": 2}"#;
        let collection: Collection = serde_json::from_str(json).expect("populated Members");
        assert_eq!(collection.members, vec!["a".to_owned(), "b".to_owned()]);
        assert_eq!(collection.count, 2);
    }

    #[test]
    fn test_required_collection_still_required() {
        let json = r#"{"Members@odata.count": 0}"#;
        serde_json::from_str::<Collection>(json)
            .expect_err("a missing Members field stays an error");
    }

    #[test]
    fn test_required_collection_rejects_wrong_type() {
        let json = r#"{"Members": 42, "Members@odata.count": 0}"#;
        serde_json::from_str::<Collection>(json).expect_err("a non-array Members stays an error");
    }
}
