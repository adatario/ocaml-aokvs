(*
 * SPDX-FileCopyrightText: 2022 Tarides <contact@tarides.com>
 *
 * SPDX-License-Identifier: ISC
 *)

(** {0 Omdb} *)

(** Omdb is an ordered key-value store implemented in pure OCaml. *)

type key = string
type value = string
type t

val init : unit -> t
val update : t -> key -> (value option -> value option) -> unit
val find : t -> key -> value option
