(*
 * SPDX-FileCopyrightText: 2022 Tarides <contact@tarides.com>
 *
 * SPDX-License-Identifier: ISC
 *)

(** Page *)

type key = int
type value = string
type record = key * value

(** {1 Identifier} *)

type id

val pp_id : id Fmt.t

(** {1 Page Types} *)

module Leaf : sig
  type t

  val pp : t Fmt.t
  val empty : t
  val sort : t -> t
  val add : key -> value -> t -> t
  val find : key -> t -> record option
  val mem : key -> t -> bool
  val count : t -> int
  val delete : key -> t -> t
  val split : t -> t * t
  val min_key : t -> key
  val max_key : t -> key
end

module Node : sig
  type entry = { left : id; pivot : key }
  type t = { entries : entry array; right : id }

  val of_two_leaves : id -> key -> id -> t

  val child : int -> t -> id
  (** [child i node] returns the [i]th child of the node. *)

  val pp : t Fmt.t
  val search : key -> t -> int
  val replace : old:id -> new':id -> t -> t
end

(** {1 Pages} *)

module PageMap : Map.S with type key = id

type t = Node of Node.t | Leaf of Leaf.t

val pp : t Fmt.t

type pages = t PageMap.t ref

val get : pages -> id -> t
val get_node : pages -> id -> Node.t
val get_leaf : pages -> id -> Leaf.t
val set : pages -> id -> t -> unit

(** {1 Allocator} *)

module Allocator : sig
  (** A monadic page allocator - a rough idea. A probably much neater
  and worked-out way of doing this: https://okmij.org/ftp/Haskell/regions.html *)

  type 'a t

  val return : 'a -> 'a t
  val alloc : id t
  val map : 'a t -> ('a -> 'b) -> 'b t
  val ( let+ ) : 'a t -> ('a -> 'b) -> 'b t
  val bind : 'a t -> ('a -> 'b t) -> 'b t
  val ( let* ) : 'a t -> ('a -> 'b t) -> 'b t

  module Unsafe : sig
    val run : int -> 'a t -> 'a * int
  end
end
