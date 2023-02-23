(*
 * SPDX-FileCopyrightText: 2023 Tarides <contact@tarides.com>
 *
 * SPDX-License-Identifier: ISC
 *)

type key = string
type value = string
type record = key * value

let pp_key = Fmt.string
let pp_value = Fmt.(quote string)
let pp_record = Fmt.(parens @@ pair ~sep:comma pp_key pp_value)
let compare_record (a_key, _) (b_key, _) = String.compare a_key b_key
