(*
 * SPDX-FileCopyrightText: 2022 Tarides <contact@tarides.com>
 *
 * SPDX-License-Identifier: ISC
 *)

open Eio

let main file =
  (* let pool = Aokvs.Page.Pool.init file in *)
  let db = Aokvs.init file in

  let inserts =
    [
      (0, "0");
      (21, String.make 1024 'a');
      (* (23, String.make 1024 'b'); *)
      (* (24, String.make 1024 'c'); *)
      (* (25, String.make 1024 'd') *)
      (5, "0");
      (6, "0");
      (* (11, 0); *)
      (* (4, 0); *)
      (* (7, 0); *)
      (* (50, 0); *)
      (* (1, 0); *)
      (* (2, 0); *)
      (* (51, 0); *)
    ]
  in

  (* let _page = Aokvs.Page.Pool.get_page pool 0 in *)

  (* ignore inserts *)
  List.iter
    (fun (key, value) ->
      Aokvs.update db (string_of_int key) (Fun.const @@ Option.some @@ value))
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
  traceln "%a" Fmt.(option string) @@ Aokvs.find db "0"

let () =
  Eio_main.run @@ fun env ->
  let path = Path.(Stdenv.cwd env / "db") in
  Path.with_open_out ~create:(`If_missing 0o600) path (fun file -> main file)
