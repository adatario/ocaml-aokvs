(*
 * SPDX-FileCopyrightText: 2022 Tarides <contact@tarides.com>
 *
 * SPDX-License-Identifier: ISC
 *)

open Bigarray

(** {0 Omdb} *)

(** Omdb is an ordered key-value store implemented in pure OCaml. *)

type memory_map = (char, int8_unsigned_elt, c_layout) Array2.t
type key = string
type value = string
type t

val init : memory_map -> t
val update : t -> key -> (value option -> value option) -> unit
val find : t -> key -> value option

module Page = Page
