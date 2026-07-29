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

use crate::edmx::ActionName;
use crate::edmx::Annotation;
use crate::edmx::IsBound;
use crate::edmx::Parameter;
use crate::edmx::ReturnType;
use crate::edmx::ValidateError;
use serde::Deserialize;

/// 12.1 Element edm:Action
#[derive(Debug, Deserialize)]
pub struct DeAction {
    /// 12.1.1 Attribute `Name`
    #[serde(rename = "@Name")]
    pub name: ActionName,
    /// 12.1.2 Attribute `IsBound`
    #[serde(rename = "@IsBound")]
    pub is_bound: Option<IsBound>,
    /// Items of edm:NavigationProperty
    #[serde(rename = "$value", default)]
    pub items: Vec<DeActionItem>,
}

#[derive(Debug, Deserialize)]
pub enum DeActionItem {
    /// The action MAY specify a return type using the edm:ReturnType element.
    ReturnType(ReturnType),
    /// The action may also define zero or more edm:Parameter
    Parameter(Parameter),
    /// Annotations can be in any property.
    Annotation(Annotation),
}

/// Validated edm:Action element.
#[derive(Debug)]
pub struct Action {
    pub name: ActionName,
    pub is_bound: IsBound,
    pub annotations: Vec<Annotation>,
    pub parameters: Vec<Parameter>,
    pub return_type: Option<ReturnType>,
}

impl DeAction {
    /// # Errors
    ///
    /// `ValidateError::NavigationProperty` error if more than one edmx:OnDelete specified.
    pub fn validate(self) -> Result<Action, ValidateError> {
        let (mut return_types, parameters, annotations) = self.items.into_iter().fold(
            (Vec::new(), Vec::new(), Vec::new()),
            |(mut rts, mut ps, mut anns), v| {
                match v {
                    DeActionItem::ReturnType(v) => rts.push(v),
                    DeActionItem::Parameter(v) => ps.push(v),
                    DeActionItem::Annotation(v) => anns.push(v),
                }
                (rts, ps, anns)
            },
        );
        if return_types.len() > 1 {
            return Err(ValidateError::Action(
                self.name,
                Box::new(ValidateError::TooManyReturnTypes),
            ));
        }
        let return_type = return_types.pop();
        Ok(Action {
            name: self.name,
            is_bound: self.is_bound.unwrap_or(IsBound::new(false)),
            return_type,
            parameters,
            annotations,
        })
    }
}
