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
          let mmap =
            Unix.map_file fd Char c_layout true [| -1; page_size |]
            |> array2_of_genarray
          in
          f mmap file
      | None -> failwith "could not open file")

let main memory_map file =
  ignore file;
  traceln "\nmmap dim: %d" (Array2.dim2 memory_map);

  let db = Omdb.init memory_map in

  let inserts =
    [
      (0, 0);
      (3, 0);
      (10, 0);
      (5, 0);
      (6, 0);
      (11, 0);
      (4, 0);
      (7, 0);
      (50, 0);
      (1, 0);
      (* (2, 0); *)
      (* (51, 0); *)
    ]
  in

  List.iter
    (fun (key, value) ->
      Omdb.update db (string_of_int key)
        (Fun.const @@ Option.some @@ string_of_int value))
    inserts;

  (* List.iter *)
  (*   (fun (key, expected_value) -> *)
  (*     let value_opt = Omdb.find db (string_of_int key) in *)

  (*     traceln "key: %a; value: %a; expected_value: %s" Fmt.string *)
  (*       (string_of_int key) *)
  (*       Fmt.(option string) *)
  (*       value_opt *)
  (*       (string_of_int expected_value)) *)
  (*   inserts; *)

  (* Omdb.update db "hi" (Fun.const @@ Option.some "hello"); *)

  (* Omdb.set db "hi2" "hello"; *)
  (* Omdb.set db "hi3" "hello"; *)
  (* Omdb.set db "hi4" "hello"; *)
  (* Omdb.set db "hi5" "hello"; *)

  (* Omdb.set db "hi6" "six"; *)
  (* Omdb.update db "hi" (Fun.const @@ Option.some "hello 2"); *)

  (* Omdb.remove db 7; *)
  (* Omdb.remove db 8; *)
  traceln "%a" Fmt.(option string) @@ Omdb.find db "3"

let () =
  Eio_main.run @@ fun env ->
  let path = Path.(Stdenv.cwd env / "db") in
  with_file path main
