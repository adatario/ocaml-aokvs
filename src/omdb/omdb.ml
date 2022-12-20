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
  type t =
    | Node of { id : Page.id; up : t option }
    | Leaf of { id : Page.id; up : t option }

  let page_id = function Node { id; _ } -> id | Leaf { id; _ } -> id

  let of_root ~pages id =
    match Page.get pages id with
    | Page.Node _ -> Node { id; up = None }
    | Page.Leaf _ -> Leaf { id; up = None }

  let of_page ~pages up id =
    match Page.get pages id with
    | Page.Node _ -> Node { id; up = Some up }
    | Page.Leaf _ -> Leaf { id; up = Some up }

  let rec search_page ~pages key t =
    match t with
    | Leaf _ -> t
    | Node { id; _ } ->
        Page.get_node pages id |> Page.Node.search key |> of_page ~pages t
        |> search_page ~pages key

  let find_record ~pages key t =
    search_page ~pages key t |> page_id |> Page.get_leaf pages
    |> Page.Leaf.find key

  let rec replace ~pages ~old ~new' up =
    let open Page.Allocator in
    match up with
    | None -> return new'
    | Some (Node { id; up }) ->
        let node = Page.get_node pages id |> Page.Node.replace ~old ~new' in
        let* page_id = alloc in
        Page.set pages page_id (Page.Node node);
        replace ~pages ~old:id ~new':page_id up
    | Some (Leaf _) ->
        failwith "unexpected leaf: cannot replace page id in leaf"

  let insert_record ~pages key value t =
    match search_page ~pages key t with
    | Leaf { id; up } ->
        let leaf = Page.get_leaf pages id |> Page.Leaf.add key value in
        if Page.Leaf.count leaf > branching_factor then (
          Page.Allocator.(
            let left, right = Page.Leaf.split leaf in
            let* left_id = alloc in
            Page.set pages left_id (Page.Leaf left);
            let* right_id = alloc in
            Page.set pages right_id (Page.Leaf right);
            let* node_id = alloc in
            let node : Page.Node.t =
              {
                entries = [ (left_id, Page.Leaf.min_key right) ];
                right = right_id;
              }
            in
            Page.set pages node_id (Page.Node node);
            replace ~pages ~old:id ~new':node_id up))
        else
          Page.Allocator.(
            let* page_id = alloc in
            Page.set pages page_id (Page.Leaf leaf);
            replace ~pages ~old:id ~new':page_id up)
    | Node _ -> failwith "search_page returned a node"

  (* let delete_key ~allocator key t = *)
  (*   let pages = Pages.of_allocator allocator in *)
  (*   match search_page ~pages key t with *)
  (*   | Leaf { id; up } -> *)
  (*       let leaf = Pages.get_leaf pages key in *)
  (*       if Page.Leaf.mem key leaf then *)
  (*         let leaf = Page.Leaf.delete key leaf in *)
  (*         let allocator, new_id = Pages.set allocator (Page.Leaf leaf) in *)
  (*         replace ~allocator ~old:id ~new':new_id up *)
  (*       else (allocator, id) *)
  (*   | Node _ -> failwith "search_page returned a node" *)
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

(* let allocator, root = Pages.set init_allocator (Page.Leaf []) in *)
(* { pages; next_free = allocator.next_free; root } *)

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

let find db key =
  Zipper.of_root ~pages:db.pages db.root
  |> Zipper.find_record ~pages:db.pages key
  |> Option.map snd
