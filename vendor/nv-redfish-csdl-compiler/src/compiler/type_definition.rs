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

use crate::compiler::is_simple_type;
use crate::compiler::Compiled;
use crate::compiler::Error;
use crate::compiler::QualifiedName;
use crate::compiler::TypeInfo;
use crate::edmx::TypeDefinition as EdmxTypeDefinition;

/// Compiled type definition.
#[derive(Debug)]
pub struct TypeDefinition<'a> {
    /// Fully qualified type name.
    pub name: QualifiedName<'a>,
    /// Underlying type name. Always a primitive type in the `Edm`
    /// namespace.
    pub underlying_type: QualifiedName<'a>,
}

pub(crate) fn compile<'a>(
    qtype: QualifiedName<'a>,
    td: &'a EdmxTypeDefinition,
) -> Result<(Compiled<'a>, TypeInfo), Error<'a>> {
    let underlying_type = (&td.underlying_type).into();
    if is_simple_type((&td.underlying_type).into()) {
        Ok((
            Compiled::new_type_definition(TypeDefinition {
                name: qtype,
                underlying_type,
            }),
            TypeInfo::type_definition(),
        ))
    } else {
        Err(Error::TypeDefinitionOfNotPrimitiveType(underlying_type))
    }
}
