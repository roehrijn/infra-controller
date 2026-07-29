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

use crate::edmx::property::DeNavigationProperty;
use crate::edmx::Annotation;
use crate::edmx::LocalTypeName;
use crate::edmx::Property;
use crate::edmx::PropertyName;
use crate::edmx::QualifiedTypeName;
use crate::edmx::StructuralProperty;
use crate::edmx::ValidateError;
use crate::IsAbstract;
use serde::Deserialize;

/// 8.1 Element edm:EntityType
#[derive(Debug, Deserialize)]
pub struct DeEntityType {
    /// 8.1.1 Attribute Name
    #[serde(rename = "@Name")]
    pub name: LocalTypeName,
    /// 8.1.2 Attribute `BaseType`
    #[serde(rename = "@BaseType")]
    pub base_type: Option<QualifiedTypeName>,
    /// 8.1.3 Attribute `Abstract`
    #[serde(rename = "@Abstract")]
    pub r#abstract: Option<IsAbstract>,
    /// 8.1.4 Attribute `OpenType`
    #[serde(rename = "@OpenType")]
    pub open_type: Option<bool>,
    /// 8.1.5 Attribute `HasStream`
    #[serde(rename = "@HasStream")]
    pub has_stream: Option<bool>,
    /// Items of edm:EntityType
    #[serde(rename = "$value", default)]
    pub items: Vec<DeEntityTypeItem>,
}

/// 8.2 Element edm:Key
#[derive(Debug, Deserialize)]
pub struct Key {
    /// Items of edm:Key
    #[serde(rename = "PropertyRef", default)]
    pub property_ref: Vec<PropertyRef>,
}

/// 8.3 Element edm:PropertyRef
#[derive(Debug, Deserialize)]
pub struct PropertyRef {
    /// 8.3.1 Attribute Name
    #[serde(rename = "@Name")]
    pub name: PropertyName,
    /// 8.3.2 Attribute Alias
    #[serde(rename = "@Alias")]
    pub alias: Option<PropertyName>,
}

/// Items of edm:EntityType
#[derive(Debug, Deserialize)]
pub enum DeEntityTypeItem {
    Key(Key),
    #[serde(rename = "Property")]
    StructuralProperty(StructuralProperty),
    NavigationProperty(DeNavigationProperty),
    Annotation(Annotation),
}

/// Validated edm:EntityType
#[derive(Debug)]
pub struct EntityType {
    pub name: LocalTypeName,
    pub base_type: Option<QualifiedTypeName>,
    pub key: Option<Key>,
    pub properties: Vec<Property>,
    pub annotations: Vec<Annotation>,
    pub is_abstract: IsAbstract,
}

impl DeEntityType {
    /// # Errors
    ///
    /// - `ValidateError::EntityType` if error occured. Internal `ValidateError` contains details.
    pub fn validate(self) -> Result<EntityType, ValidateError> {
        let (keys, properties, annotations) = self.items.into_iter().fold(
            (Vec::new(), Vec::new(), Vec::new()),
            |(mut keys, mut ps, mut anns), v| {
                match v {
                    DeEntityTypeItem::Key(k) => {
                        keys.push(k);
                    }
                    DeEntityTypeItem::StructuralProperty(p) => ps.push(p.validate()),
                    DeEntityTypeItem::NavigationProperty(p) => ps.push(p.validate()),
                    DeEntityTypeItem::Annotation(a) => anns.push(a),
                }
                (keys, ps, anns)
            },
        );
        if keys.len() > 1 {
            return Err(ValidateError::EntityType(
                self.name,
                Box::new(ValidateError::TooManyKeys),
            ));
        }
        let name = self.name;
        let properties = properties
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| ValidateError::EntityType(name.clone(), Box::new(e)))?;
        let key = keys.into_iter().next();
        Ok(EntityType {
            name,
            key,
            base_type: self.base_type,
            properties,
            annotations,
            is_abstract: self.r#abstract.unwrap_or(IsAbstract::new(false)),
        })
    }
}
