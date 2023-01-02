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
    (* the parent node and the position in parent that points here *)
    (* TODO: there's a bit more type safety we could gain. We know
       that if there is an up, it is a node. e.g:

       parent: (node t * int) option
    *)
    parent : (t * int) option;
    pages : Page.pages;
  }

  let pp ppf =
    Fmt.(
      pf ppf "@[<7><Zipper %a>@]"
      @@ record
           [
             field "id" (fun t -> t.id) Page.pp_id;
             field "page" (fun t -> t.page) Page.pp;
             field "parent[id]" (fun t ->
                 Option.map (fun (t, _) -> t.id) t.parent)
             @@ option Page.pp_id;
             field "parent[pos]" (fun t -> Option.map snd t.parent)
             @@ option int;
           ])

  let of_root ~pages id = { page = Page.get pages id; id; parent = None; pages }

  let rec to_root t =
    match t with
    | { parent = None; _ } -> t
    | { parent = Some (up, _); _ } -> to_root up

  let root_child t : Page.Node.child =
    let root = to_root t in
    let id = root.id in
    let min_key = Page.min_key root.page in
    { id; min_key }

  let rec search_page key t =
    traceln "search_page - key: %a, zipper: %a" pp_key key pp t;
    match t.page with
    | Leaf _ -> t
    | Node node ->
        let pos, id = Page.Node.search key node in
        { t with page = Page.get t.pages id; id; parent = Some (t, pos) }
        |> search_page key

  let find_record key t =
    match search_page key t with
    | { page = Leaf leaf; _ } ->
        Page.Leaf.find_pos leaf key |> Option.map (Page.Leaf.get leaf)
    | _ -> failwith "search_page returned a node"

  let rec replace_in_parent (new_child : Page.Node.child) t =
    let open Page.Allocator in
    match t.parent with
    | Some (({ page = Node node; _ } as grand_parent), pos) ->
        let* new_id = Node.replace_child ~pos new_child node in
        replace_in_parent new_id grand_parent
    | Some ({ page = Leaf _; _ }, _) -> failwith "parent is a leaf"
    | None -> return new_child

  let rec split_in_parent left_child right_child t =
    let open Page.Allocator in
    match t.parent with
    | Some (({ page = Node node; _ } as grand_parent), pos) -> (
        (* split child in node *)
        let* either_split = Node.split_child ~pos left_child right_child node in

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
        Node.make left_child right_child

  let rec insert_record key value t =
    traceln "Omdb.insert_record ~key:%a ~value:%a %a" pp_key key pp_value value
      pp t;
    let open Page.Allocator in
    match t with
    | { page = Leaf leaf; parent = None; _ } -> (
        (* root leaf *)
        let* either_split = Leaf.add key value leaf in
        match either_split with
        | Either.Left id ->
            (* return new leaf as root *)
            return id
        | Either.Right (left_id, right_id) ->
            (* allocate a new node with two children as root *)
            Node.make left_id right_id)
    | { page = Leaf leaf; _ } -> (
        let* either_split = Leaf.add key value leaf in
        match either_split with
        | Either.Left id -> replace_in_parent id t
        | Either.Right (left_id, right_id) -> split_in_parent left_id right_id t
        )
    | { page = Node _; _ } -> search_page key t |> insert_record key value

  let replace_record_value ~pos value t =
    let open Page.Allocator in
    match t with
    | { page = Leaf leaf; _ } ->
        let* id = Leaf.replace_value ~pos value leaf in
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
        match Page.Leaf.find_pos leaf key with
        (* record exists *)
        | Some pos -> (
            let value = Page.Leaf.get leaf pos |> snd in
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

let dump db =
  Page.PageMap.iter
    (fun id page -> traceln "id: %a, page: %a" Page.pp_id id Page.pp page)
    !(db.pages);

  traceln "root: %a" Page.pp_id db.root

let init _ =
  let pages = ref Page.PageMap.empty in

  let root, next_free =
    Page.Allocator.Unsafe.run ~pages ~next_free:0 Page.Allocator.(Leaf.alloc [])
  in

  { pages; next_free; root = root.id }

let update db key f =
  let root, next_free =
    Zipper.of_root ~pages:db.pages db.root
    |> Zipper.update key f
    |> Page.Allocator.Unsafe.run ~pages:db.pages ~next_free:db.next_free
  in

  db.next_free <- next_free;
  db.root <- root.id;

  dump db

let find db key =
  Zipper.of_root ~pages:db.pages db.root
  |> Zipper.find_record key |> Option.map snd
