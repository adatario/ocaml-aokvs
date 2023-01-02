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

  val find_pos : t -> key -> int option
  (** [find_pos leaf key] returns the position of the record with
      associated [key] in [leaf]. *)

  val get : t -> int -> record
  (** [get leaf pos] returns the record at position [pos] in [leaf]. *)

  (** {1 Pretty-printing} *)

  val pp : t Fmt.t
end

module Node : sig
  type t
  type child = { id : id; min_key : key }

  (** {1 Accessors} *)

  val count : t -> int

  val child : int -> t -> id
  (** [child i node] returns the [i]th child of the node. *)

  val search : key -> t -> int * id
  (** [search key node] returns the id of the child. *)

  (** {1 Pretty-printing} *)

  val pp : t Fmt.t
end

(** {1 Page} *)

type t = Node of Node.t | Leaf of Leaf.t

val to_node : t -> Node.t option
val to_leaf : t -> Leaf.t option
val min_key : t -> key

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

  type split = (Node.child, Node.child * Node.child) Either.t

  module Leaf : sig
    val alloc : record list -> Node.child t
    val add : key -> value -> Leaf.t -> split t
    val replace_value : pos:int -> value -> Leaf.t -> Node.child t
  end

  module Node : sig
    val make : Node.child -> Node.child -> Node.child t
    (** [make child1 child2] allocates a new node page with children
    [child1] and [child2]. *)

    val replace_child : pos:int -> Node.child -> Node.t -> Node.child t
    (** [replace_child ~pos child node] allocates a new node page
    where the child at position [pos] is replaced with [child]. *)

    val split_child : pos:int -> Node.child -> Node.child -> Node.t -> split t
    (** [split_child ~pos child1 child2] allocates a new node page
    where the child at position [pos] is replaced with the two
    children [child1] and [child2]. *)
  end

  module Unsafe : sig
    val run : pages:pages -> next_free:int -> 'a t -> 'a * int
  end
end
