(*
 * SPDX-FileCopyrightText: 2022 Tarides <contact@tarides.com>
 *
 * SPDX-License-Identifier: ISC
 *)

open Eio

type key = string
type value = string

let pp_key = Fmt.(string)
let pp_value = Fmt.(quote string)

module Zipper = struct
  type t = {
    page : Page.t;
    id : Page.id;
    (* the upper node and the child position *)
    (* TODO: there's a bit more type safety we could gain. We know
       that if there is an up, it is a node. e.g:

       up : (node t * int) option
    *)
    up : (t * int) option;
    pages : Page.pages;
  }

  let pp =
    Fmt.(
      record
        [
          field "id" (fun t -> t.id) Page.pp_id;
          field "page" (fun t -> t.page) Page.pp;
          field "up[id]" (fun t -> Option.map (fun (t, _) -> t.id) t.up)
          @@ option Page.pp_id;
          field "up[pos]" (fun t -> Option.map snd t.up) @@ option int;
        ])

  let page_id t = t.id
  let of_root ~pages id = { page = Page.get pages id; id; up = None; pages }

  let rec search_page key t =
    traceln "search_page - key: %a, zipper: %a" pp_key key pp t;
    match t.page with
    | Leaf _ -> t
    | Node node ->
        let pos, id = Page.Node.search key node in
        traceln "search_page - pos: %d, id: %a" pos Page.pp_id id;
        { t with page = Page.get t.pages id; id; up = Some (t, pos) }
        |> search_page key

  let find_record key t =
    search_page key t |> page_id |> Page.get t.pages |> Page.to_leaf
    |> (Fun.flip Option.bind) (Page.Leaf.find key)

  (* Replace a single child in a node *)
  let rec replace_child ~pos child t =
    traceln "replce_child - child: %a, zipper: %a" Page.pp_id child pp t;
    let open Page.Allocator in
    match t with
    | { page = Node node; up = None; _ } ->
        (* Node is root, replace with a new root and return. *)
        Node.replace_child ~pos child node
    | { page = Node node; up = Some (up, up_pos); _ } ->
        (* replace in current node and continue up the tree *)
        let* new_node_id = Node.replace_child ~pos child node in
        replace_child ~pos:up_pos new_node_id up
    | { page = Leaf _; _ } ->
        failwith "unexpected leaf: cannot replace page id in leaf"

  (* Replace a single child with 2 children *)
  let rec split_child ~pos child1 child2 t =
    let open Page.Allocator in
    match t with
    | { page = Node node; up = None; _ } -> (
        (* we are the current root *)
        let* either_split = Node.split_child ~pos child1 child2 node in
        match either_split with
        | Either.Left id ->
            (* no need to split, we have a new root *)
            return id
        | Either.Right (left_id, right_id) ->
            (* create a new root node with two children *)
            Node.make left_id right_id)
    | { page = Node node; up = Some (up, up_pos); _ } -> (
        (* split the child and recurse up the tree *)
        let* either_split = Node.split_child ~pos child1 child2 node in
        match either_split with
        | Either.Left id -> replace_child ~pos:up_pos id up
        | Either.Right (left_id, right_id) ->
            split_child ~pos:up_pos left_id right_id up)
    | { page = Leaf _; _ } -> failwith "replace_with_twins: unexpected leaf"

  let rec insert_record key value t =
    traceln "insert_record - key: %a, value: %a, zipper: %a" pp_key key pp_value
      value pp t;
    let open Page.Allocator in
    match t with
    | { page = Leaf leaf; up = None; _ } -> (
        (* root leaf *)
        let* either_split = Leaf.add key value leaf in
        match either_split with
        | Either.Left id ->
            (* return new leaf as root *)
            return id
        | Either.Right (left_id, right_id) ->
            (* allocate a new node with two children as root *)
            Node.make left_id right_id)
    | { page = Leaf leaf; up = Some (up, up_pos); _ } -> (
        let* either_split = Leaf.add key value leaf in
        match either_split with
        | Either.Left id -> replace_child ~pos:up_pos id up
        | Either.Right (left_id, right_id) ->
            split_child ~pos:up_pos left_id right_id up)
    | { page = Node _; _ } -> search_page key t |> insert_record key value

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
  traceln "set - key: %s, value: %s, root: %a" key value Page.pp root;

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
