(*
 * SPDX-FileCopyrightText: 2022 Tarides <contact@tarides.com>
 *
 * SPDX-License-Identifier: ISC
 *)
open Bigarray

(** Page *)

type key = string
type value = string
type record = key * value
type memory_map = (char, int8_unsigned_elt, c_layout) Array2.t

(** {1 Page} *)

type page

(** {2 Identifier} *)

type id

val id_equal : id -> id -> bool
val pp_id : id Fmt.t

(** {2 Getting a page} *)

val get_page : memory_map -> id -> page

(** {1 Allocator} *)

(** A monadic page allocator - a rough idea. A probably much neater
  and worked-out way of doing this: https://okmij.org/ftp/Haskell/regions.html *)

module Allocator : sig
  type 'a t

  (** {1 Combinators} *)

  val return : 'a -> 'a t
  val map : 'a t -> ('a -> 'b) -> 'b t
  val ( let+ ) : 'a t -> ('a -> 'b) -> 'b t
  val bind : 'a t -> ('a -> 'b t) -> 'b t
  val ( let* ) : 'a t -> ('a -> 'b t) -> 'b t

  (** {1 Page retrieval} *)

  val get_page : id -> page t

  module Unsafe : sig
    val run : memory_map:memory_map -> next_free:int -> 'a t -> 'a * int
  end
end

(* {1 Helper Types } *)

type child = { id : id; min_key : key }
(** Type of a child entry in a node. *)

type split = (child, child * child) Either.t
(** type for allocations that might split a page into two *)

(** {1 Page Types} *)

module Leaf : sig
  (** A page that holds records *)

  type t

  val of_page : page -> t option

  (** {1 Accessors} *)

  val count : t -> int
  (** [count leaf] returns the number of records (key-value
  pairs) contained in the leaf page [leaf].*)

  val find_position : t -> key -> int option
  (** [find_pos leaf key] returns the position of the record with
      associated [key] in [leaf]. *)

  val get_record : t -> int -> record
  (** [get leaf pos] returns the record at position [pos] in [leaf]. *)

  val records : t -> record Seq.t
  val min_key : t -> key
  val free_space : t -> int

  (** {1 Allocatioon} *)

  val empty : id Allocator.t
  val singleton : record -> child Allocator.t
  val add : t -> record -> split Allocator.t
  val replace_value : t -> pos:int -> value -> child Allocator.t

  (** {1 Pretty-printing} *)

  val pp : t Fmt.t
end

module Node : sig
  type t

  val of_page : page -> t option

  (** {1 Accessors} *)

  val count : t -> int

  val child_id : t -> int -> id
  (** [child_id node i] returns the page id of the [i]th child of the node. *)

  val children : t -> child Seq.t
  (** [children node] returns a sequence of children contained in the node. *)

  val search : t -> key -> int * id
  (** [search node key] returns the id of the child. *)

  val min_key : t -> key

  (** {1 Allocators} *)

  val make : child -> child -> child Allocator.t
  (** [make child1 child2] returns an allocator that creates a new
  node page with children [child1] and [child2]. *)

  val replace_child : t -> pos:int -> child -> child Allocator.t
  val split_child : t -> pos:int -> child -> child -> split Allocator.t

  (** {1 Pretty-printing} *)

  val pp : t Fmt.t
end

(* (\** {1 Page} *\) *)

(* val to_node : t -> Node.t option *)
(* val to_leaf : t -> Leaf.t option *)
(* val min_key : t -> key *)

(* type t_id = id * t *)

(* val get_leaf : pages -> id -> Leaf.t *)
(* val get_node : pages -> id -> Node.t *)

(* module Allocator : sig *)
(*   type 'a t *)

(*   val return : 'a -> 'a t *)
(*   val id : unit -> id t *)
(*   val map : 'a t -> ('a -> 'b) -> 'b t *)
(*   val ( let+ ) : 'a t -> ('a -> 'b) -> 'b t *)
(*   val bind : 'a t -> ('a -> 'b t) -> 'b t *)
(*   val ( let* ) : 'a t -> ('a -> 'b t) -> 'b t *)

(*   type split = (Node.child, Node.child * Node.child) Either.t *)

(*   module Leaf : sig *)
(*     val alloc : record list -> Node.child t *)
(*     val add : key -> value -> Leaf.t -> split t *)
(*     val replace_value : pos:int -> value -> Leaf.t -> Node.child t *)
(*   end *)

(*   module Node : sig *)
(*     val make : Node.child -> Node.child -> Node.child t *)
(*     (\** [make child1 child2] allocates a new node page with children *)
(*     [child1] and [child2]. *\) *)

(*     val replace_child : pos:int -> Node.child -> Node.t -> Node.child t *)
(*     (\** [replace_child ~pos child node] allocates a new node page *)
(*     where the child at position [pos] is replaced with [child]. *\) *)

(*     val split_child : pos:int -> Node.child -> Node.child -> Node.t -> split t *)
(*     (\** [split_child ~pos child1 child2] allocates a new node page *)
(*     where the child at position [pos] is replaced with the two *)
(*     children [child1] and [child2]. *\) *)
(*   end *)

(* end *)
