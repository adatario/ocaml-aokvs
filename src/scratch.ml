(*
 * SPDX-FileCopyrightText: 2022 Tarides <contact@tarides.com>
 *
 * SPDX-License-Identifier: ISC
 *)

open Eio
open Bigarray

let page_size = 4096
let key_size = 256

module BPTree = struct
  type key = int
  type value = string
  type record = key * value

  type children = { entries : (node * key) list; right : node }
  and node = NInternal of children | NLeaf of record list

  type t =
    | RootLeaf of record list
    | Root of children
    | Internal of { up : t; children : children }
    | Leaf of { up : t; records : record list }

  let pp ppf t =
    let pp_key = Fmt.int in
    let pp_value = Fmt.string in
    let pp_record = Fmt.(braces @@ pair ~sep:comma pp_key pp_value) in
    match t with
    | RootLeaf records ->
        Fmt.pf ppf "RootLeaf %a"
          Fmt.(brackets @@ list ~sep:semi pp_record)
          records
    | _ -> Fmt.pf ppf "@[NODE?@]"

  let init = RootLeaf []

  let rec search t key =
    let in_children children key =
      let rec iterate entries key =
        match entries with
        | (node, pivot) :: _ when key < pivot -> node
        | _ :: tail -> iterate tail key
        | [] -> children.right
      in
      match iterate children.entries key with
      | NInternal children -> search (Internal { up = t; children }) key
      | NLeaf records -> Leaf { up = t; records }
    in
    match t with
    | RootLeaf _ -> t
    | Root children -> in_children children key
    | Internal { children; _ } -> in_children children key
    | Leaf _ -> t

  let insert t key value =
    match search t key with
    | RootLeaf records -> RootLeaf ((key, value) :: records)
    | Leaf { up; records } -> Leaf { up; records = (key, value) :: records }
    | _ -> failwith "unexpected internal node"
end

let with_file path f =
  Path.with_open_out ~create:(`If_missing 0o600) path (fun file ->
      match Eio_unix.FD.peek_opt file with
      | Some fd ->
          Unix.ftruncate fd (page_size * 1024);
          let mmap = Unix.map_file fd Char c_layout true [| -1; page_size |] in
          f mmap file
      | None -> failwith "could not open file")

let main mmap file =
  ignore file;
  traceln "\nmmap dim: %a" Fmt.(array ~sep:semi int) (Genarray.dims mmap);

  let db =
    Omdb.(
      init mmap |> set 1 "hello" |> set 2 "bye" |> set 50 "wow"
      |> set 123 "nope" |> set 42 "oh yeah")
  in

  traceln "%a" Omdb.Zipper.pp db;

  traceln "%a" Fmt.(option string) @@ Omdb.find db 1

let () =
  Eio_main.run @@ fun env ->
  let path = Path.(Stdenv.cwd env / "db") in
  with_file path main
