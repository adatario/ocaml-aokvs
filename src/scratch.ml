(*
 * SPDX-FileCopyrightText: 2022 Tarides <contact@tarides.com>
 *
 * SPDX-License-Identifier: ISC
 *)

open Eio
open Bigarray

let with_file path f =
  Path.with_open_out ~create:(`If_missing 0o600) path (fun file ->
      match Eio_unix.FD.peek_opt file with
      | Some fd ->
          Unix.ftruncate fd 256;
          let mmap = Unix.map_file fd Char c_layout true [| -1; 1 |] in
          f mmap file
      | None -> failwith "could not open file")

let main mmap file =
  ignore file;
  traceln "\nmmap dim: %a" Fmt.(array ~sep:semi int) (Genarray.dims mmap)

let () =
  Eio_main.run @@ fun env ->
  let path = Path.(Stdenv.cwd env / "db") in
  with_file path main
