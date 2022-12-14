(*
 * SPDX-FileCopyrightText: 2022 Tarides <contact@tarides.com>
 *
 * SPDX-License-Identifier: ISC
 *)

type key = int
type value = string

module Record = struct
  type t = key * value

  let sort = List.stable_sort (fun (a, _) (b, _) -> Int.compare a b)
  let add records key value = (key, value) :: records |> sort

  let find key =
    List.find_opt (fun (record_key, value) ->
        if record_key = key then true else false)
end

let branching_factor = 4

module Page = struct
  type children = { entries : (t * key) list; right : t }
  and t = Internal of children | Leaf of Record.t list

  let count = function
    | Internal { entries; _ } -> List.length entries
    | Leaf records -> List.length records

  let rec search children key =
    let rec iterate entries key =
      match entries with
      | (node, pivot) :: _ when key < pivot -> node
      | _ :: tail -> iterate tail key
      | [] -> children.right
    in
    iterate children.entries key

  let replace t ~old ~new' =
    match t with
    | Internal { entries; right } ->
        Internal
          {
            entries =
              List.map
                (fun (child, key) ->
                  if child = old then (new', key) else (child, key))
                entries;
            right = (if right = old then new' else right);
          }
    | Leaf _ -> t
end

module Zipper = struct
  type t =
    | RootLeaf of Record.t list
    | Root of Page.children
    | Internal of { up : t; children : Page.children }
    | Leaf of { up : t; records : Record.t list }

  let pp ppf t =
    let pp_key = Fmt.int in
    let pp_value = Fmt.string in
    let pp_record = Fmt.(parens @@ pair ~sep:comma pp_key pp_value) in
    match t with
    | RootLeaf records ->
        Fmt.pf ppf "RootLeaf %a"
          Fmt.(brackets @@ list ~sep:semi pp_record)
          records
    | _ -> Fmt.pf ppf "@[NODE?@]"

  let init = RootLeaf []

  let of_page up = function
    | Page.Internal children -> Internal { up; children }
    | Page.Leaf records -> Leaf { up; records }

  let to_page = function
    | RootLeaf records -> Page.Leaf records
    | Leaf { records; _ } -> Page.Leaf records
    | Internal { children; _ } -> Page.Internal children
    | Root children -> Page.Internal children

  let rec search_page t key =
    match t with
    | RootLeaf _ -> t
    | Root children -> Page.search children key |> of_page t
    | Internal { children; _ } -> Page.search children key |> of_page t
    | Leaf _ -> t

  let page_records = function
    | RootLeaf records -> records
    | Leaf { records; _ } -> records
    | Internal _ -> []
    | Root _ -> []

  let find_record t key = search_page t key |> page_records |> Record.find key

  let split_records records =
    List.(
      mapi
        (fun i record ->
          if i <= branching_factor / 2 then Either.left record
          else Either.right record)
        records
      |> partition_map Fun.id)

  let rec replace_child t ~old ~new' =
    match t with
    | Root _ -> (
        match Page.replace (to_page t) ~old ~new' with
        | Page.Internal children -> Root children
        | Page.Leaf _ -> failwith "expecting a root")
    | Internal { up; _ } ->
        replace_child up ~old:(to_page t)
          ~new':(Page.replace (to_page t) ~old ~new')
    | RootLeaf _ -> t
    | Leaf _ -> t

  let insert_record t key value =
    match search_page t key with
    | RootLeaf records ->
        let records = Record.add records key value in
        RootLeaf records
    | Leaf { up; records } as old ->
        replace_child up ~old:(to_page old)
          ~new':(Page.Leaf (Record.add records key value))
    | _ -> failwith "unexpected internal node"
end

let init _ = Zipper.init
let set key value db = Zipper.insert_record db key value
let find db key = Zipper.find_record db key |> Option.map snd
