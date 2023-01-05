(*
 * SPDX-FileCopyrightText: 2022 Tarides <contact@tarides.com>
 *
 * SPDX-License-Identifier: ISC
 *)

open Eio
open Bigarray
include Record

type memory_map = (char, int8_unsigned_elt, c_layout) Array2.t

module Zipper = struct
  type page = Leaf of Page.Leaf.t | Node of Page.Node.t

  let page_pp ppf page =
    match page with
    | Leaf leaf -> Fmt.pf ppf "@[<5><Leaf %a>@]" Page.Leaf.pp leaf
    | Node node -> Fmt.pf ppf "@[<5><Node %a>@]" Page.Node.pp node

  let get_page memory_map id =
    let page = Page.get_page memory_map id in
    match Page.Leaf.of_page page with
    | Some leaf -> Leaf leaf
    | None -> (
        match Page.Node.of_page page with
        | Some node -> Node node
        | None -> failwith "expecting page to be leaf or node")

  let min_key page =
    match page with
    | Leaf leaf -> Page.Leaf.min_key leaf
    | Node node -> Page.Node.min_key node

  type t = {
    page : page;
    id : Page.id;
    (* the parent node and the position in parent that points here *)
    (* TODO: there's a bit more type safety we could gain. We know
       that if there is an up, it is a node. e.g:

       parent: (node t * int) option
    *)
    parent : (t * int) option;
    memory_map : memory_map;
  }

  let pp ppf =
    Fmt.(
      pf ppf "@[<7><Zipper %a>@]"
      @@ record
           [
             field "id" (fun t -> t.id) Page.pp_id;
             field "page" (fun t -> t.page) page_pp;
             field "parent[id]" (fun t ->
                 Option.map (fun (t, _) -> t.id) t.parent)
             @@ option Page.pp_id;
             field "parent[pos]" (fun t -> Option.map snd t.parent)
             @@ option int;
           ])

  let of_root ~memory_map id =
    { page = get_page memory_map id; id; parent = None; memory_map }

  let rec to_root t =
    match t with
    | { parent = None; _ } -> t
    | { parent = Some (up, _); _ } -> to_root up

  let root_child t : Page.child =
    let root = to_root t in
    let id = root.id in
    let min_key = min_key root.page in
    { id; min_key }

  let rec search_page key t =
    traceln "search_page - key: %a, zipper: %a" pp_key key pp t;
    match t.page with
    | Leaf _ -> t
    | Node node ->
        let pos, id = Page.Node.search node key in
        { t with page = get_page t.memory_map id; id; parent = Some (t, pos) }
        |> search_page key

  let find_record key t =
    match search_page key t with
    | { page = Leaf leaf; _ } ->
        Page.Leaf.find_position leaf key
        |> Option.map (Page.Leaf.get_record leaf)
    | _ -> failwith "search_page returned a node"

  let rec replace_in_parent (new_child : Page.child) t =
    let open Page.Allocator in
    match t.parent with
    | Some (({ page = Node node; _ } as grand_parent), pos) ->
        let* new_id = Page.Node.replace_child node ~pos new_child in
        replace_in_parent new_id grand_parent
    | Some ({ page = Leaf _; _ }, _) -> failwith "parent is a leaf"
    | None -> return new_child

  let rec split_in_parent left_child right_child t =
    let open Page.Allocator in
    match t.parent with
    | Some (({ page = Node node; _ } as grand_parent), pos) -> (
        (* split child in node *)
        let* either_split =
          Page.Node.split_child node ~pos left_child right_child
        in

        match either_split with
        | Either.Left new_id ->
            (* node did not split, replace new id in grand parent *)
            replace_in_parent new_id grand_parent
        | Either.Right (left_id, right_id) ->
            (* node split into two nodes, replace in grand parent *)
            split_in_parent left_id right_id grand_parent)
    | Some ({ page = Leaf _; _ }, _) -> failwith "parent is a leaf"
    | None ->
        (* create a new root node with two children *)
        Page.Node.make left_child right_child

  let rec insert_record key value t =
    traceln "Omdb.insert_record ~key:%a ~value:%a %a" pp_key key pp_value value
      pp t;
    let open Page.Allocator in
    match t with
    | { page = Leaf leaf; parent = None; _ } -> (
        (* root leaf *)
        let* either_split = Page.Leaf.add leaf (key, value) in
        match either_split with
        | Either.Left id ->
            (* return new leaf as root *)
            return id
        | Either.Right (left_id, right_id) ->
            (* allocate a new node with two children as root *)
            Page.Node.make left_id right_id)
    | { page = Leaf leaf; _ } -> (
        let* either_split = Page.Leaf.add leaf (key, value) in
        match either_split with
        | Either.Left id -> replace_in_parent id t
        | Either.Right (left_id, right_id) -> split_in_parent left_id right_id t
        )
    | { page = Node _; _ } -> search_page key t |> insert_record key value

  let replace_record_value ~pos value t =
    let open Page.Allocator in
    match t with
    | { page = Leaf leaf; _ } ->
        let* id = Page.Leaf.replace_value leaf ~pos value in
        replace_in_parent id t
    | _ -> failwith "called replace record value on node"

  let delete_record ~pos t =
    let open Page.Allocator in
    ignore pos;
    return @@ root_child t

  let rec update key f t =
    match t with
    | { page = Leaf leaf; _ } -> (
        let open Page.Allocator in
        match Page.Leaf.find_position leaf key with
        (* record exists *)
        | Some pos -> (
            let value = Page.Leaf.get_record leaf pos |> snd in
            match f (Some value) with
            | Some new_value when value <> new_value ->
                (* replace value *)
                replace_record_value ~pos new_value t
            | Some _ ->
                (* nothing to do, return the root id *)
                return @@ root_child t
            | None ->
                (* delete record *)
                delete_record ~pos t)
        (* no record yet*)
        | None -> (
            match f None with
            | Some value ->
                (* insert a new record *)
                insert_record key value t
            | None ->
                (* nothing to do, return the root id *)
                return @@ root_child t))
    | { page = Node _; _ } ->
        (* not yet at leaf *)
        search_page key t |> update key f
end

type t = {
  memory_map : memory_map;
  mutable next_free : int;
  mutable root : Page.id;
}

let init memory_map =
  (* TODO: check dimensions *)
  let root, next_free =
    Page.Allocator.Unsafe.run ~memory_map ~next_free:0 Page.Leaf.empty
  in

  { memory_map; next_free; root }

let update db key f =
  let root, next_free =
    Zipper.of_root ~memory_map:db.memory_map db.root
    |> Zipper.update key f
    |> Page.Allocator.Unsafe.run ~memory_map:db.memory_map
         ~next_free:db.next_free
  in

  db.next_free <- next_free;
  db.root <- root.id

let find db key =
  Zipper.of_root ~memory_map:db.memory_map db.root
  |> Zipper.find_record key |> Option.map snd

module Page = Page
