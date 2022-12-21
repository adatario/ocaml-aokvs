(*
 * SPDX-FileCopyrightText: 2022 Tarides <contact@tarides.com>
 *
 * SPDX-License-Identifier: ISC
 *)

open Eio

type key = int
type value = string
type record = key * value

let pp_key = Fmt.int
let pp_value = Fmt.string
let pp_record = Fmt.(parens @@ pair ~sep:comma pp_key pp_value)
let page_size = 4096
let branching_factor = 4

module Zipper = struct
  type t = { page : Page.t; pos : int; id : Page.id; up : t option }

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
    match Page.get pages id with
    | Page.Node _ as page -> { page; id; up = None; pos = 0 }
    | Page.Leaf _ as page -> { page; id; up = None; pos = 0 }

  let of_page ~pages ?(pos = 0) up id =
    match Page.get pages id with
    | Page.Node _ as page -> { page; id; up = Some up; pos }
    | Page.Leaf _ as page -> { page; id; up = Some up; pos }

  let rec search_page ~pages key t =
    match t.page with
    | Leaf _ -> t
    | Node node ->
        let pos = Page.Node.search key node in
        let id = Page.Node.child pos node in
        of_page ~pages ~pos t id |> search_page ~pages key

  let find_record ~pages key t =
    search_page ~pages key t |> page_id |> Page.get_leaf pages
    |> Page.Leaf.find key

  let rec replace ~pages ~old ~new' up =
    let open Page.Allocator in
    match up with
    | None -> return new'
    | Some { page = Node node; id; up; _ } ->
        let node' = Page.Node.replace ~old ~new' node in
        let* page_id = alloc in
        Page.set pages page_id (Page.Node node');
        replace ~pages ~old:id ~new':page_id up
    | Some { page = Leaf _; _ } ->
        failwith "unexpected leaf: cannot replace page id in leaf"

  let insert_record ~pages key value t =
    match search_page ~pages key t with
    | { page = Leaf leaf; id; up; _ } ->
        let leaf' = Page.Leaf.add key value leaf in
        if Page.Leaf.count leaf' > branching_factor then (
          Page.Allocator.(
            let left, right = Page.Leaf.split leaf' in
            let* left_id = alloc in
            Page.set pages left_id (Page.Leaf left);
            let* right_id = alloc in
            Page.set pages right_id (Page.Leaf right);
            let* node_id = alloc in
            let node =
              Page.Node.of_two_leaves left_id (Page.Leaf.min_key right) right_id
            in
            Page.set pages node_id (Page.Node node);
            replace ~pages ~old:id ~new':node_id up))
        else
          Page.Allocator.(
            let* page_id = alloc in
            Page.set pages page_id (Page.Leaf leaf');
            replace ~pages ~old:id ~new':page_id up)
    | { page = Node _; _ } -> failwith "search_page returned a node"

  let merge_leaf ~pages leaf t =
    let open Page.Allocator in
    ignore pages;
    traceln "merge_leaf - t: %a, leaf: %a" pp t Page.Leaf.pp leaf;
    return t.id

  let rec remove_key ~pages key t =
    let open Page.Allocator in
    match t with
    | { page = Leaf leaf; _ } when not (Page.Leaf.mem key leaf) ->
        (* Nothing to do *)
        return @@ root_id t
    | { page = Leaf leaf; _ } ->
        let leaf' = Page.Leaf.delete key leaf in
        if Page.Leaf.count leaf' < branching_factor / 2 then (
          match t.up with
          | Some up -> merge_leaf ~pages leaf' up
          | None ->
              (* we are the root *)
              let* new_id = alloc in
              Page.set pages new_id (Page.Leaf leaf');
              return new_id)
        else
          let* new_id = alloc in
          Page.set pages new_id (Page.Leaf leaf');
          replace ~pages ~old:t.id ~new':new_id t.up
    | { page = Node _; _ } ->
        (* descend to leaf node and try again *)
        search_page ~pages key t |> remove_key ~pages key
end

type t = { pages : Page.pages; mutable next_free : int; mutable root : Page.id }

let init _ =
  let pages = ref Page.PageMap.empty in

  let root, next_free =
    Page.Allocator.Unsafe.run 0
      Page.Allocator.(
        let+ root_id = alloc in
        Page.set pages root_id (Page.Leaf Page.Leaf.empty);
        root_id)
  in

  { pages; next_free; root }

let set db key value =
  let root = Page.get db.pages db.root in
  traceln "root: %a" Page.pp root;

  let zipper = Zipper.of_root ~pages:db.pages db.root in

  let root', next_free =
    Page.Allocator.Unsafe.run db.next_free
    @@ Zipper.insert_record ~pages:db.pages key value zipper
  in

  db.next_free <- next_free;
  db.root <- root'

let remove db key =
  let root = Page.get db.pages db.root in
  traceln "remove - root: %a" Page.pp root;
  let zipper = Zipper.of_root ~pages:db.pages db.root in

  let new_root_id, next_free =
    Page.Allocator.Unsafe.run db.next_free
    @@ Zipper.remove_key ~pages:db.pages key zipper
  in

  let root = Page.get db.pages new_root_id in
  traceln "remove - new_root: %a" Page.pp root;

  db.next_free <- next_free;
  db.root <- new_root_id

let find db key =
  let root = Page.get db.pages db.root in
  traceln "find - root: %a" Page.pp root;
  Zipper.of_root ~pages:db.pages db.root
  |> Zipper.find_record ~pages:db.pages key
  |> Option.map snd
