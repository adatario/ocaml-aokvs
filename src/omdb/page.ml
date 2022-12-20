(*
 * SPDX-FileCopyrightText: 2022 Tarides <contact@tarides.com>
 *
 * SPDX-License-Identifier: ISC
 *)

type key = int
type value = string
type record = key * value

let pp_key = Fmt.int
let pp_value = Fmt.string
let pp_record = Fmt.(parens @@ pair ~sep:comma pp_key pp_value)
let branching_factor = 4

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

  let empty = []
  let sort = List.stable_sort (fun (a, _) (b, _) -> Int.compare a b)
  let add key value leaf = (key, value) :: leaf |> sort

  let find key =
    List.find_opt (fun (record_key, _value) ->
        if record_key = key then true else false)

  let mem key leaf = find key leaf |> Option.is_some
  let count t = List.length t
  let delete key = List.filter (fun (record_key, _value) -> key <> record_key)

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

module PageMap = Map.Make (Int)

type t = Node of Node.t | Leaf of Leaf.t

let pp ppf page =
  match page with
  | Node node -> Fmt.pf ppf "@[Node %a@]" Node.pp node
  | Leaf records ->
      Fmt.pf ppf "@[Leaf %a@]"
        Fmt.(brackets @@ list ~sep:semi pp_record)
        records

type pages = t PageMap.t ref

let get pages id = PageMap.find id !pages

let set pages id page =
  pages := PageMap.update id (Fun.const @@ Some page) !pages

let get_node pages id =
  match PageMap.find id !pages with
  | Node node -> node
  | _ -> failwith "expecting node in get_node"

let get_leaf pages id =
  match PageMap.find id !pages with
  | Leaf leaf -> leaf
  | _ -> failwith "expecting leaf in get_leaf"

module Allocator = struct
  type allocator = { next_free : id }
  type 'a t = allocator -> 'a * allocator

  let return v allocator = (v, allocator)

  let alloc allocator =
    (allocator.next_free, { next_free = allocator.next_free + 1 })

  let map a f allocator =
    let v, allocator' = a allocator in
    (f v, allocator')

  let ( let+ ) = map

  let bind a f allocator =
    let a_v, a_allocator = a allocator in
    (f a_v) a_allocator

  let ( let* ) = bind

  module Unsafe = struct
    let run next_free t =
      let v, { next_free } = t { next_free } in
      (v, next_free)
  end
end
