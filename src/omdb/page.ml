(*
 * SPDX-FileCopyrightText: 2022 Tarides <contact@tarides.com>
 *
 * SPDX-License-Identifier: ISC
 *)

open Eio

type key = string
type value = string
type record = key * value

let pp_key = Fmt.string
let pp_value = Fmt.(quote string)
let pp_record = Fmt.(parens @@ pair ~sep:comma pp_key pp_value)
let branching_factor = 4

(** Page *)

type id = int

let pp_id = Fmt.(styled `Cyan @@ int)

(* Page Types *)

module Leaf = struct
  type t = record list

  let pp = Fmt.(brackets @@ list ~sep:semi pp_record)
  let sort = List.stable_sort (fun (a, _) (b, _) -> String.compare a b)
  let add key value leaf = (key, value) :: leaf |> sort
  let count t = List.length t

  (* NOTE: find_pos and get could be combined to a find. However, once we
     serialize to disk it will make sense to seperate the two calls. *)

  let find_pos leaf key =
    leaf |> List.to_seq
    |> Seq.mapi (fun i record -> (i, record))
    |> Seq.find_map (fun (i, (r_key, _)) ->
           if r_key = key then Some i else None)

  let get leaf pos = List.nth leaf pos

  (* let delete key = List.filter (fun (record_key, _value) -> key <> record_key) *)

  let split records =
    List.(
      mapi
        (fun i record ->
          if i <= branching_factor / 2 then Either.left record
          else Either.right record)
        records
      |> partition_map Fun.id)

  let min_key records = match records with (key, _) :: _ -> key | [] -> "SDFS"
end

module Node = struct
  type child = { id : id; min_key : key }
  type t = child list

  let pp =
    Fmt.(
      brackets @@ list ~sep:semi
      @@ record
           [
             field "id" (fun t -> t.id) pp_id;
             field "min_key" (fun t -> t.min_key) pp_key;
           ])

  let count node = List.length node
  let child i node = List.nth node i |> fun child -> child.id

  let search key node =
    (* Sequence of pivots of the node. The lenght of the sequence is
       equal to the lenght of children. The last pivot is None. *)
    let pivots =
      node |> List.to_seq
      (* the first min_key is not used as pivot *)
      |> Seq.drop 1
      |> Seq.map (fun child -> Option.some child.min_key)
      (* the last child does not need a pivot *)
      |> fun pivots -> Seq.(append pivots (return None))
    in

    let child_ids = node |> List.to_seq |> Seq.map (fun child -> child.id) in
    Seq.zip child_ids pivots
    |> Seq.mapi (fun pos (child, pivot) -> (pos, child, pivot))
    |> Seq.find_map (fun (pos, child, pivot) ->
           match pivot with
           | Some pivot -> if key < pivot then Some (pos, child) else None
           | None -> Some (pos, child))
    |> Option.get

  let replace_child ~pos new_child node =
    List.mapi (fun i c -> if i = pos then new_child else c) node

  let min_key node =
    match node with
    | { min_key; _ } :: _ -> min_key
    | _ -> failwith "node can not be empty"
end

type t = Node of Node.t | Leaf of Leaf.t

let to_node = function Node node -> Some node | _ -> None
let to_leaf = function Leaf leaf -> Some leaf | _ -> None

let min_key = function
  | Node node -> Node.min_key node
  | Leaf leaf -> Leaf.min_key leaf

type t_id = id * t

module PageMap = Map.Make (Int)

type pages = t PageMap.t ref

let pp ppf page =
  match page with
  | Node node -> Fmt.pf ppf "@[<5><Node %a>@]" Node.pp node
  | Leaf records -> Fmt.pf ppf "@[<5><Leaf %a>@]" Leaf.pp records

let get pages id = PageMap.find id !pages

let get_node pages id =
  match PageMap.find id !pages with
  | Node node -> node
  | _ -> failwith "expecting node in get_node"

let get_leaf pages id =
  match PageMap.find id !pages with
  | Leaf leaf -> leaf
  | _ -> failwith "expecting leaf in get_leaf"

(* Page Allocator *)

module Allocator = struct
  type allocator = { next_free : id; pages : pages }
  type 'a t = allocator -> 'a * allocator

  let return v allocator = (v, allocator)

  let id () allocator =
    (allocator.next_free, { allocator with next_free = allocator.next_free + 1 })

  let set_page page id : Node.child t =
   fun allocator ->
    traceln "set_page  ~id:%a ~page:%a" pp_id id pp page;
    let min_key = min_key page in
    allocator.pages :=
      PageMap.update id (Fun.const @@ Some page) !(allocator.pages);
    ({ id; min_key }, allocator)

  let map a f allocator =
    let v, allocator' = a allocator in
    (f v, allocator')

  let ( let+ ) = map

  let bind a f allocator =
    let a_v, a_allocator = a allocator in
    (f a_v) a_allocator

  let ( let* ) = bind

  type split = (Node.child, Node.child * Node.child) Either.t

  module Leaf = struct
    let alloc records =
      let* id = id () in
      set_page (Leaf records) id

    let add key value leaf =
      let leaf' = Leaf.add key value leaf in
      if Leaf.count leaf' > branching_factor then
        let left, right = Leaf.split leaf' in
        let* left_id = alloc left in
        let* right_id = alloc right in
        return @@ Either.right (left_id, right_id)
      else
        let* id = alloc leaf' in
        return @@ Either.left id

    let replace_value ~pos value leaf =
      leaf |> List.to_seq
      |> Seq.mapi (fun i (r_key, r_value) ->
             if i = pos then (r_key, value) else (r_key, r_value))
      |> List.of_seq |> alloc
  end

  module Node = struct
    let alloc node =
      let* id = id () in
      set_page (Node node) id

    let make child1 child2 = alloc [ child1; child2 ]

    let replace_child ~pos child (node : Node.t) =
      Node.replace_child ~pos child node |> alloc

    let split_child ~pos child1 child2 (node : Node.t) =
      let child_count = Node.count node + 1 in
      let left_seq, right_seq =
        node |> List.to_seq
        (* Insert the new children *)
        |> Seq.mapi (fun i child ->
               if i = pos - 1 then Seq.return child
               else if i = pos then List.to_seq [ child1; child2 ]
               else Seq.return child)
        |> Seq.concat
        (* Split node *)
        |> Seq.mapi (fun i child_pivot ->
               if child_count > branching_factor then
                 (* we need to split the node *)
                 if i <= child_count / 2 then Either.left child_pivot
                 else Either.right child_pivot
               else Either.left child_pivot)
        |> Seq.partition_map Fun.id
      in
      if Seq.is_empty right_seq then
        let node = List.of_seq left_seq in
        let* id = alloc node in
        return @@ Either.left id
      else
        let left = List.of_seq left_seq in
        let right = List.of_seq right_seq in
        let* left_id = alloc left in
        let* right_id = alloc right in
        return @@ Either.right (left_id, right_id)
  end

  module Unsafe = struct
    let run ~pages ~next_free t =
      let v, { next_free; _ } = t { next_free; pages } in
      (v, next_free)
  end
end
