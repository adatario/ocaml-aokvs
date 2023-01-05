(*
 * SPDX-FileCopyrightText: 2023 Tarides <contact@tarides.com>
 *
 * SPDX-License-Identifier: ISC
 *)

open Bigarray

let make_memory_map = Array2.create char c_layout 1024 4096

let record_testable =
  Alcotest.testable
    Fmt.(parens @@ pair ~sep:comma string string)
    (fun (key_a, value_a) (key_b, value_b) ->
      String.equal key_a key_b && String.equal value_a value_b)

module Leaf = struct
  let page_size = 4096

  let expected_header_size =
    (* magic number *)
    2
    (* count *)
    + 2
    (* max_offset *)
    + 2
    (* padding *)
    + 2
    (* left *)
    + 8
    (* right *)
    + 8

  let expected_record_entry_size =
    (* key_offset *)
    2 (* key_length *) + 2
    (* value_offset *)
    + 2
    (* value_length *)
    + 2

  let test_allocate_empty =
    Alcotest.test_case "allocate an empty leaf page" `Quick (fun () ->
        let memory_map = make_memory_map in

        let id, _next_free =
          Omdb.Page.Allocator.Unsafe.run ~memory_map ~next_free:0
            Omdb.Page.Leaf.empty
        in

        let page = Omdb.Page.get_page memory_map id in
        let leaf = Omdb.Page.Leaf.of_page page |> Option.get in

        Alcotest.(check int "count is 0" 0 @@ Omdb.Page.Leaf.count leaf);

        Alcotest.(
          check int "free space is page size minus header"
            (page_size - expected_header_size)
          @@ Omdb.Page.Leaf.free_space leaf))

  let test_allocate_singleton =
    Alcotest.test_case "allocate a singleton" `Quick (fun () ->
        let memory_map = make_memory_map in

        let key = "MYKEY" in
        let value = "Hello OMDB!" in

        let child, _next_free =
          Omdb.Page.Allocator.Unsafe.run ~memory_map ~next_free:0
          @@ Omdb.Page.Leaf.singleton (key, value)
        in

        let page = Omdb.Page.get_page memory_map child.id in
        let leaf = Omdb.Page.Leaf.of_page page |> Option.get in

        Alcotest.(check int "count is 1" 1 @@ Omdb.Page.Leaf.count leaf);

        Alcotest.(
          check int "free space is as expected"
            (page_size - expected_header_size - expected_record_entry_size
           - String.length key - String.length value)
          @@ Omdb.Page.Leaf.free_space leaf);

        Alcotest.(
          check record_testable "record can be retrieved" (key, value)
            (Omdb.Page.Leaf.get_record leaf 0)))

  let test_cases = [ test_allocate_empty; test_allocate_singleton ]
end

let () = Alcotest.run "Omdb.Page" [ ("Leaf", Leaf.test_cases) ]
