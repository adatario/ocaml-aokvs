(*
 * SPDX-FileCopyrightText: 2022 Tarides <contact@tarides.com>
 *
 * SPDX-License-Identifier: ISC
 *)

(** Page *)

type key = string
type value = string
type record = key * value

(** {1 Identifier} *)

type id

val pp_id : id Fmt.t

(** {1 Page Types} *)

module Leaf : sig
  (** A page that holds records *)

  type t

  (** {1 Accessors} *)

  val count : t -> int
  (** [count leaf] returns the number of records (key-value
  pairs) contained in the leaf page [leaf].*)

  val find : key -> t -> record option
  (** [find key leaf] returns the record associated with [key] in [leaf]. *)

  val mem : key -> t -> bool
  val min_key : t -> key

  (** {1 Pretty-printing} *)

  val pp : t Fmt.t
end

module Node : sig
  type t

  (** {1 Accessors} *)

  val count : t -> int

  val child : int -> t -> id
  (** [child i node] returns the [i]th child of the node. *)

  val search : key -> t -> int * id
  (** [search key node] returns the id of the child. *)

  val min_key : t -> key

  (** {1 Pretty-printing} *)

  val pp : t Fmt.t
end

(** {1 Page} *)

type t = Node of Node.t | Leaf of Leaf.t

val to_node : t -> Node.t option
val to_leaf : t -> Leaf.t option

type t_id = id * t

val pp : t Fmt.t

module PageMap : Map.S with type key = id

type pages = t PageMap.t ref

val get : pages -> id -> t
val get_leaf : pages -> id -> Leaf.t
val get_node : pages -> id -> Node.t

(** {1 Allocator} *)

(** A monadic page allocator - a rough idea. A probably much neater
  and worked-out way of doing this: https://okmij.org/ftp/Haskell/regions.html *)

module Allocator : sig
  type 'a t

  val return : 'a -> 'a t
  val id : unit -> id t
  val map : 'a t -> ('a -> 'b) -> 'b t
  val ( let+ ) : 'a t -> ('a -> 'b) -> 'b t
  val bind : 'a t -> ('a -> 'b t) -> 'b t
  val ( let* ) : 'a t -> ('a -> 'b t) -> 'b t

  type split = (id, id * id) Either.t

  module Leaf : sig
    val alloc : record list -> id t
    val add : key -> value -> Leaf.t -> split t
  end

  module Node : sig
    val make : id -> id -> id t
    (** [make child1 child2] allocates a new node page with children
    [child1] and [child2]. *)

    val replace_child : pos:int -> id -> Node.t -> id t
    (** [replace_child ~pos child node] allocates a new node page
    where the child at position [pos] is replaced with [child]. *)

    val split_child : pos:int -> id -> id -> Node.t -> split t
    (** [split_child ~pos child1 child2] allocates a new node page
    where the child at position [pos] is replaced with the two
    children [child1] and [child2]. *)
  end

  module Unsafe : sig
    val run : pages:pages -> next_free:int -> 'a t -> 'a * int
  end
end
