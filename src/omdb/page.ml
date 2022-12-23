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

  let min_key records =
    let min, _ = List.hd records in
    min
end

module Node = struct
  type t = { children : id list; pivots : key list }

  let pp =
    Fmt.(
      record
        [
          field "children" (fun t -> t.children)
          @@ brackets @@ list ~sep:semi pp_id;
          field "pivots" (fun t -> t.pivots)
          @@ brackets @@ list ~sep:semi pp_key;
        ])

  let count node = List.length node.children
  let child i node = List.nth node.children i
  let right node = child (count node - 1) node

  let search key node =
    Seq.zip (List.to_seq node.children) (List.to_seq node.pivots)
    |> Seq.mapi (fun pos (child, pivot) -> (pos, child, pivot))
    |> Seq.find_map (fun (pos, child, pivot) ->
           if key < pivot then Some (pos, child) else None)
    |> Option.value ~default:(count node - 1, right node)

  let replace_child ~pos ~pivot child node =
    let pivot_pos = pos - 1 in
    let pivots =
      List.mapi (fun i p -> if i = pivot_pos then pivot else p) node.pivots
    in
    let children =
      List.mapi (fun i c -> if i = pos then child else c) node.children
    in
    { children; pivots }
end

type t = Node of Node.t | Leaf of Leaf.t

let to_node = function Node node -> Some node | _ -> None
let to_leaf = function Leaf leaf -> Some leaf | _ -> None

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

  let set_page page id allocator =
    traceln "set_page  ~id:%a ~page:%a" pp_id id pp page;
    allocator.pages :=
      PageMap.update id (Fun.const @@ Some page) !(allocator.pages);
    (id, allocator)

  let get_page id allocator = (PageMap.find id !(allocator.pages), allocator)

  let map a f allocator =
    let v, allocator' = a allocator in
    (f v, allocator')

  let ( let+ ) = map

  let bind a f allocator =
    let a_v, a_allocator = a allocator in
    (f a_v) a_allocator

  let ( let* ) = bind

  type split = (id, id * id) Either.t

  (* TODO: merge Allocator.{Leaf,Node} with the modules above. *)
  let leaf_min_key = Leaf.min_key

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

    let rec get_min_key id =
      let* page = get_page id in
      traceln "Node.get_min_key ~id:%a ~page:%a" pp_id id pp page;
      match page with
      | Leaf leaf -> return @@ leaf_min_key leaf
      | Node { children = child :: _; _ } -> get_min_key child
      | Node { children = []; _ } -> failwith "node does not have children"

    let make child1 child2 =
      let* pivot = get_min_key child2 in
      alloc { children = [ child1; child2 ]; pivots = [ pivot ] }

    let replace_child ~pos child (node : Node.t) =
      let* pivot = get_min_key child in
      Node.replace_child ~pos ~pivot child node |> alloc

    let max_pivot = String.make 256 (Char.chr 255)

    let node_of_seq seq : Node.t =
      let children_seq, pivots_seq = Seq.unzip seq in
      let children = List.of_seq children_seq in
      let pivots =
        List.of_seq @@ Seq.take (List.length children - 1) pivots_seq
      in
      assert (List.length pivots = List.length children - 1);
      { children; pivots }

    let split_child ~pos child1 child2 (node : Node.t) =
      let* pivot1 = get_min_key child1 in
      let* pivot2 = get_min_key child2 in
      let child_count = Node.count node + 1 in
      let left_seq, right_seq =
        Seq.zip
          (List.to_seq node.children)
          (Seq.append (List.to_seq node.pivots) (Seq.return max_pivot))
        (* Insert the new children *)
        |> Seq.mapi (fun i (child, pivot) ->
               if i = pos - 1 then Seq.return (child, pivot1)
               else if i = pos then
                 List.to_seq [ (child1, pivot2); (child2, pivot) ]
               else Seq.return (child, pivot))
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
        let node = node_of_seq left_seq in
        let* id = alloc node in
        return @@ Either.left id
      else
        let left = node_of_seq left_seq in
        let right = node_of_seq right_seq in
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
