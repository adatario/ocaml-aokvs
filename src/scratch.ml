(*
 * SPDX-FileCopyrightText: 2022 Tarides <contact@tarides.com>
 *
 * SPDX-License-Identifier: ISC
 *)

open Eio
open Bigarray

let page_size = 4096

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

  let db = Omdb.init () in

  Omdb.set db "hi" "hello";
  Omdb.set db "hi2" "hello";
  Omdb.set db "hi3" "hello";
  Omdb.set db "hi4" "hello";
  Omdb.set db "hi5" "hello";
  Omdb.set db "hi6" "hello";

  (* Omdb.remove db 7; *)
  (* Omdb.remove db 8; *)
  traceln "%a" Fmt.(option string) @@ Omdb.find db "hi"

let () =
  Eio_main.run @@ fun env ->
  let path = Path.(Stdenv.cwd env / "db") in
  with_file path main
