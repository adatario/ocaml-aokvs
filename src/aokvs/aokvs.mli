(*
 * SPDX-FileCopyrightText: 2022 Tarides <contact@tarides.com>
 *
 * SPDX-License-Identifier: ISC
 *)

(** {0 Aokvs} *)

type key = string
type value = string
type t

val init : #Eio.File.rw -> t
val update : t -> key -> (value option -> value option) -> unit
val find : t -> key -> value option

module Page = Page
