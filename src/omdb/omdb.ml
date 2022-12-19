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

module PageMap = Map.Make (Int)

module Page = struct
  type id = int

  let pp_id = Fmt.(styled `Cyan @@ int)

  module Node = struct
    type t = { entries : (id * key) list; right : id }

    let pp ppf node =
      let pp_entries =
        Fmt.(brackets @@ list ~sep:semi @@ parens @@ pair ~sep:comma int pp_key)
      in
      Fmt.(pf ppf "%a; %a" pp_entries node.entries int node.right)

    let search key node =
      let rec iterate entries key =
        match entries with
        | (node, pivot) :: _ when key < pivot -> node
        | _ :: tail -> iterate tail key
        | [] -> node.right
      in
      iterate node.entries key

    let replace ~old ~new' { entries; right } =
      {
        entries =
          List.map
            (fun (child, key) ->
              if child = old then (new', key) else (child, key))
            entries;
        right = (if right = old then new' else right);
      }
  end

  module Leaf = struct
    type t = record list

    let pp = Fmt.list
    let sort = List.stable_sort (fun (a, _) (b, _) -> Int.compare a b)
    let add key value leaf = (key, value) :: leaf |> sort

    let find key =
      List.find_opt (fun (record_key, _value) ->
          if record_key = key then true else false)

    let count t = List.length t

    let split records =
      List.(
        mapi
          (fun i record ->
            if i <= branching_factor / 2 then Either.left record
            else Either.right record)
          records
        |> partition_map Fun.id)

    let min_key = function
      | [ (min, _); _ ] -> min
      | _ -> failwith "record is empty"

    let max_key records =
      match List.(nth records (length records - 1)) with max, _ -> max
  end

  type t = Node of Node.t | Leaf of Leaf.t

  let pp ppf page =
    match page with
    | Node node -> Fmt.pf ppf "@[Node %a@]" Node.pp node
    | Leaf records ->
        Fmt.pf ppf "@[Leaf %a@]"
          Fmt.(brackets @@ list ~sep:semi pp_record)
          records

  let count = function
    | Node { entries; _ } -> List.length entries
    | Leaf records -> List.length records
end

module Pages = struct
  type t = Page.t PageMap.t ref
  type allocator = { pages : t; next_free : Page.id }

  let get pages id = PageMap.find id !pages

  let set allocator page =
    let page_ref = allocator.pages in
    let id = allocator.next_free in
    page_ref := PageMap.update id (Fun.const @@ Some page) !page_ref;
    ({ allocator with next_free = allocator.next_free + 1 }, id)

  let of_allocator allocator = allocator.pages

  let get_node pages id =
    match PageMap.find id !pages with
    | Page.Node node -> node
    | _ -> failwith "expecting node in get_node"

  let get_leaf pages id =
    match PageMap.find id !pages with
    | Page.Leaf leaf -> leaf
    | _ -> failwith "expecting leaf in get_leaf"
end

module Zipper = struct
  type t =
    | Node of { id : Page.id; up : t option }
    | Leaf of { id : Page.id; up : t option }

  let page_id = function Node { id; _ } -> id | Leaf { id; _ } -> id

  let of_root ~pages id =
    match Pages.get pages id with
    | Page.Node _ -> Node { id; up = None }
    | Page.Leaf _ -> Leaf { id; up = None }

  let of_page ~pages up id =
    match Pages.get pages id with
    | Page.Node _ -> Node { id; up = Some up }
    | Page.Leaf _ -> Leaf { id; up = Some up }

  let rec search_page ~pages key t =
    match t with
    | Leaf _ -> t
    | Node { id; _ } ->
        Pages.get_node pages id |> Page.Node.search key |> of_page ~pages t
        |> search_page ~pages key

  let find_record ~pages key t =
    search_page ~pages key t |> page_id |> Pages.get_leaf pages
    |> Page.Leaf.find key

  let rec replace ~allocator ~old ~new' up =
    let pages = Pages.of_allocator allocator in
    match up with
    | None -> (allocator, new')
    | Some (Node { id; up }) ->
        let node = Pages.get_node pages id |> Page.Node.replace ~old ~new' in
        let allocator, page_id = Pages.set allocator (Page.Node node) in
        replace ~allocator ~old:id ~new':page_id up
    | Some (Leaf _) ->
        failwith "unexpected leaf: cannot replace page id in leaf"

  let insert_record ~allocator key value t =
    let pages = Pages.of_allocator allocator in
    match search_page ~pages key t with
    | Leaf { id; up } ->
        let leaf = Pages.get_leaf pages id |> Page.Leaf.add key value in
        if Page.Leaf.count leaf > branching_factor then
          let left, right = Page.Leaf.split leaf in
          let allocator, left_id = Pages.set allocator (Page.Leaf left) in
          let allocator, right_id = Pages.set allocator (Page.Leaf right) in
          let allocator, node_id =
            Pages.set allocator
              (Page.Node
                 {
                   entries = [ (left_id, Page.Leaf.min_key right) ];
                   right = right_id;
                 })
          in
          replace ~allocator ~old:id ~new':node_id up
        else
          let allocator, page_id = Pages.set allocator (Page.Leaf leaf) in
          replace ~allocator ~old:id ~new':page_id up
    | Node _ -> failwith "unexpected node"
end

type t = {
  pages : Pages.t;
  mutable next_free : Page.id;
  mutable root : Page.id;
}

let init _ =
  let pages = ref PageMap.empty in
  let init_allocator : Pages.allocator = { pages; next_free = 0 } in
  let allocator, root = Pages.set init_allocator (Page.Leaf []) in
  { pages; next_free = allocator.next_free; root }

let set db key value =
  let allocator : Pages.allocator =
    { pages = db.pages; next_free = db.next_free }
  in

  let root = Pages.get db.pages db.root in
  traceln "root: %a" Page.pp root;

  let zipper = Zipper.of_root ~pages:db.pages db.root in

  let allocator, root' = Zipper.insert_record ~allocator key value zipper in

  db.next_free <- allocator.next_free;
  db.root <- root'

let find db key =
  Zipper.of_root ~pages:db.pages db.root
  |> Zipper.find_record ~pages:db.pages key
  |> Option.map snd
