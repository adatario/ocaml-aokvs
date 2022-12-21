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
let pp_value = Fmt.string
let pp_record = Fmt.(parens @@ pair ~sep:comma pp_key pp_value)
let page_size = 4096
let branching_factor = 4

module Zipper = struct
  type t = {
    page : Page.t;
    pos : int;
    id : Page.id;
    up : t option;
    pages : Page.pages;
  }

  let pp =
    Fmt.(
      record
        [
          field "id" (fun t -> t.id) Page.pp_id;
          field "page" (fun t -> t.page) Page.pp;
          field "pos" (fun t -> t.pos) int;
          field "up[id]" (fun t -> Option.map (fun t -> t.id) t.up)
          @@ option Page.pp_id;
          field "up[pos]" (fun t -> Option.map (fun t -> t.pos) t.up)
          @@ option int;
        ])

  let page_id t = t.id
  let rec root_id t = match t.up with None -> t.id | Some up -> root_id up

  let of_root ~pages id =
    { page = Page.get pages id; pos = 0; id; up = None; pages }

  let rec search_page key t =
    match t.page with
    | Leaf _ -> t
    | Node node ->
        let pos, id = Page.Node.search key node in
        { t with page = Page.get t.pages id; id; pos; up = Some t }
        |> search_page key

  let find_record key t =
    search_page key t |> page_id |> Page.get t.pages |> Page.to_leaf
    |> (Fun.flip Option.bind) (Page.Leaf.find key)

  (* Replace a single child in a node *)
  let rec replace_child child t_opt =
    let open Page.Allocator in
    match t_opt with
    | None ->
        (* child is the new root *)
        return child
    | Some { page = Node node; up; pos; _ } ->
        (* replace in current node and continue up the tree *)
        let* new_node_id = Node.replace_child ~pos child node in
        replace_child new_node_id up
    | Some { page = Leaf _; _ } ->
        failwith "unexpected leaf: cannot replace page id in leaf"

  (* Replace a single child with 2 children *)
  let rec split_child child1 child2 t_opt =
    let open Page.Allocator in
    match t_opt with
    | None ->
        (* allocate a new root node *)
        Node.make child1 child2
    | Some { page = Node node; pos; up; _ } -> (
        let* either_split = Node.split_child ~pos child1 child2 node in
        match either_split with
        | Either.Left id -> replace_child id up
        | Either.Right (left_id, right_id) -> split_child left_id right_id up)
    | Some { page = Leaf _; _ } ->
        failwith "replace_with_twins: unexpected leaf"

  let insert_record key value t =
    traceln "insert_record %a %a" pp_key key pp_value value;
    let open Page.Allocator in
    match search_page key t with
    | { page = Leaf leaf; up; _ } -> (
        let* either_split = Leaf.add key value leaf in
        match either_split with
        | Either.Left id -> replace_child id t.up
        | Either.Right (left_id, right_id) -> split_child left_id right_id up)
    | { page = Node _; _ } -> failwith "search_page returned a node"

  (* let merge_leaf ~pages leaf t = *)
  (*   let open Page.Allocator in *)
  (*   ignore pages; *)
  (*   traceln "merge_leaf - t: %a, leaf: %a" pp t Page.Leaf.pp leaf; *)
  (*   return t.id *)

  (* let rec remove_key ~pages key t = *)
  (*   let open Page.Allocator in *)
  (*   match t with *)
  (*   | { page = Leaf leaf; _ } when not (Page.Leaf.mem key leaf) -> *)
  (*       (\* Nothing to do *\) *)
  (*       return @@ root_id t *)
  (*   | { page = Leaf leaf; _ } -> *)
  (*       let leaf' = Page.Leaf.delete key leaf in *)
  (*       if Page.Leaf.count leaf' < branching_factor / 2 then ( *)
  (*         match t.up with *)
  (*         | Some up -> merge_leaf ~pages leaf' up *)
  (*         | None -> *)
  (*             (\* we are the root *\) *)
  (*             let* new_id = alloc in *)
  (*             Page.set pages new_id (Page.Leaf leaf'); *)
  (*             return new_id) *)
  (*       else *)
  (*         let* new_id = alloc in *)
  (*         Page.set pages new_id (Page.Leaf leaf'); *)
  (*         replace ~pages ~old:t.id ~new':new_id t.up *)
  (*   | { page = Node _; _ } -> *)
  (*       (\* descend to leaf node and try again *\) *)
  (*       search_page ~pages key t |> remove_key ~pages key *)
end

type t = { pages : Page.pages; mutable next_free : int; mutable root : Page.id }

let init _ =
  let pages = ref Page.PageMap.empty in

  let root, next_free =
    Page.Allocator.Unsafe.run ~pages ~next_free:0 Page.Allocator.(Leaf.alloc [])
  in

  { pages; next_free; root }

let set db key value =
  let root = Page.get db.pages db.root in
  traceln "root: %a" Page.pp root;

  let zipper = Zipper.of_root ~pages:db.pages db.root in

  let root', next_free =
    Page.Allocator.Unsafe.run ~pages:db.pages ~next_free:db.next_free
    @@ Zipper.insert_record key value zipper
  in

  db.next_free <- next_free;
  db.root <- root'

(* let remove db key = *)
(*   let root = Page.get db.pages db.root in *)
(*   traceln "remove - root: %a" Page.pp root; *)
(*   let zipper = Zipper.of_root ~pages:db.pages db.root in *)

(*   let new_root_id, next_free = *)
(*     Page.Allocator.Unsafe.run db.next_free *)
(*     @@ Zipper.remove_key ~pages:db.pages key zipper *)
(*   in *)

(*   let root = Page.get db.pages new_root_id in *)
(*   traceln "remove - new_root: %a" Page.pp root; *)

(*   db.next_free <- next_free; *)
(*   db.root <- new_root_id *)

let find db key =
  traceln "find %a" pp_key key;
  let root = Page.get db.pages db.root in
  traceln "find - root: %a" Page.pp root;
  Zipper.of_root ~pages:db.pages db.root
  |> Zipper.find_record key |> Option.map snd
