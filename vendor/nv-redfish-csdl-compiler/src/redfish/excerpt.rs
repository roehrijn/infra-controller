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

use std::collections::HashSet;
use tagged_types::TaggedType;

/// Identifier of the Excerpt.
pub type ExcerptKey = TaggedType<String, ExcerptKeyTag>;
#[doc(hidden)]
#[derive(tagged_types::Tag)]
#[implement(Clone, Copy, Hash, PartialEq, Eq)]
#[transparent(Display, Debug)]
#[capability(inner_access)]
pub enum ExcerptKeyTag {}

/// Defines excerpt status of the property.
#[derive(Debug)]
pub enum Excerpt {
    /// Property is included in any excerpt copy.
    All,
    /// Property is include in copies with the specified keys.
    Keys(HashSet<ExcerptKey>),
}

impl Excerpt {
    #[must_use]
    pub fn matches(&self, copy: &ExcerptCopy) -> bool {
        match self {
            Self::All => true,
            Self::Keys(set) => match copy {
                ExcerptCopy::AllKeys => true,
                ExcerptCopy::Key(v) => set.contains(v),
            },
        }
    }
}

/// Excerpt copy of the resource.
///
/// Defines what kind of excerpt copy of the resource property
/// contains. `AllKeys` defines that all attribures marked as Excerpt
/// shall be included. If specific key is defined then only attributes
/// marked with `ExcerptKey` must be included.
#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub enum ExcerptCopy {
    AllKeys,
    Key(ExcerptKey),
}
