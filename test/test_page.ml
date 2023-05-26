(*
 * SPDX-FileCopyrightText: 2023 Tarides <contact@tarides.com>
 *
 * SPDX-License-Identifier: ISC
 *)

let with_pool f =
  (* TODO: use a better temporary location *)
  Eio_main.run @@ fun env ->
  let path = Eio.Path.(Eio.Stdenv.cwd env / "test.db") in
  Eio.Path.with_open_out ~create:(`If_missing 0o600) path (fun file ->
      let pool = Aokvs.Page.Pool.init file in

      f pool);
  try Eio.Path.unlink path with _ -> ()

let record_testable =
  Alcotest.testable
    Fmt.(parens @@ pair ~sep:comma string (truncated ~max:8))
    (fun (key_a, value_a) (key_b, value_b) ->
      String.equal key_a key_b && String.equal value_a value_b)

let child_testable =
  Alcotest.testable
    Fmt.(
      parens
      @@ record
           [
             field "id"
               (fun (child : Aokvs.Page.child) -> child.id)
               Aokvs.Page.pp_id;
             field "min_key"
               (fun (child : Aokvs.Page.child) -> child.min_key)
               string;
           ])
    (fun a b ->
      Aokvs.Page.id_equal a.id b.id && String.equal a.min_key b.min_key)

(* Helpers to create records *)

let test_record i = ("KEY_" ^ string_of_int i, "VALUE_" ^ string_of_int i)

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
        with_pool (fun pool ->
            let id, _next_free =
              Aokvs.Page.Allocator.Unsafe.run ~pool ~next_free:0
                Aokvs.Page.Leaf.empty
            in

            let page = Aokvs.Page.Pool.get_page pool id in
            let leaf = Aokvs.Page.Leaf.of_page page |> Option.get in

            Alcotest.(check int "count is 0" 0 @@ Aokvs.Page.Leaf.count leaf);

            Alcotest.(
              check int "free space is page size minus header"
                (page_size - expected_header_size)
              @@ Aokvs.Page.Leaf.free_space leaf)))

  let test_allocate_singleton =
    Alcotest.test_case "allocate a singleton" `Quick (fun () ->
        with_pool (fun pool ->
            let key, value = test_record 1 in

            let child, _next_free =
              Aokvs.Page.Allocator.Unsafe.run ~pool ~next_free:0
              @@ Aokvs.Page.Leaf.singleton (key, value)
            in

            let page = Aokvs.Page.Pool.get_page pool child.id in
            let leaf = Aokvs.Page.Leaf.of_page page |> Option.get in

            Alcotest.(check int "count is 1" 1 @@ Aokvs.Page.Leaf.count leaf);

            Alcotest.(
              check int "free space is as expected"
                (page_size - expected_header_size - expected_record_entry_size
               - String.length key - String.length value)
              @@ Aokvs.Page.Leaf.free_space leaf);

            Alcotest.(
              check record_testable "record can be retrieved" (key, value)
                (Aokvs.Page.Leaf.get_record leaf 0))))

  let test_add =
    Alcotest.test_case "add a record to an existing leaf" `Quick (fun () ->
        with_pool (fun pool ->
            let leaf_child =
              Aokvs.Page.Allocator.(
                Unsafe.run ~pool ~next_free:0
                  (let* singleton_child =
                     Aokvs.Page.Leaf.singleton (test_record 1)
                   in

                   let* page = get_page singleton_child.id in

                   let empty_leaf =
                     page |> Aokvs.Page.Leaf.of_page |> Option.get
                   in

                   Aokvs.Page.Leaf.add empty_leaf (test_record 2)))
              |> fst |> Either.find_left |> Option.get
            in

            let leaf =
              Aokvs.Page.Pool.get_page pool leaf_child.id
              |> Aokvs.Page.Leaf.of_page |> Option.get
            in

            Alcotest.(
              check (list record_testable) "all records present in leaf"
                [ test_record 1; test_record 2 ]
                (List.of_seq @@ Aokvs.Page.Leaf.records leaf));

            ()))

  let test_add_out_of_order =
    Alcotest.test_case "add records out of order" `Quick (fun () ->
        with_pool (fun pool ->
            let records =
              [ 0; 3; 6; 2; 7; 1; 5; 8; 9; 4 ] |> List.map test_record
            in

            let leaf_id, _ =
              Aokvs.Page.Allocator.(
                Unsafe.run ~pool ~next_free:0
                  (List.fold_left
                     (fun id_t record ->
                       let* id = id_t in
                       let* page = get_page id in
                       let leaf =
                         page |> Aokvs.Page.Leaf.of_page |> Option.get
                       in

                       let* child_split = Aokvs.Page.Leaf.add leaf record in
                       let child =
                         child_split |> Either.find_left |> Option.get
                       in

                       return child.id)
                     Aokvs.Page.Leaf.empty records))
            in

            let leaf =
              Aokvs.Page.Pool.get_page pool leaf_id
              |> Aokvs.Page.Leaf.of_page |> Option.get
            in

            Alcotest.(
              check (list record_testable) "check records present are in order"
                (List.init 10 test_record)
                (List.of_seq @@ Aokvs.Page.Leaf.records leaf));

            ()))

  let test_add_with_split =
    Alcotest.test_case "add records and cause a split" `Quick (fun () ->
        with_pool (fun pool ->
            (* test records are big (256 bytes values) *)
            let test_record i =
              ("KEY_" ^ string_of_int i, String.make 1024 'X')
            in

            let left_child, right_child =
              Aokvs.Page.Allocator.(
                Unsafe.run ~pool ~next_free:0
                  (let* empty_id = Aokvs.Page.Leaf.empty in

                   let* page = get_page empty_id in
                   let leaf = page |> Aokvs.Page.Leaf.of_page |> Option.get in

                   let* child_split =
                     Aokvs.Page.Leaf.add leaf (test_record 0)
                   in
                   let child = child_split |> Either.find_left |> Option.get in
                   let* page = get_page child.id in
                   let leaf = page |> Aokvs.Page.Leaf.of_page |> Option.get in

                   let* child_split =
                     Aokvs.Page.Leaf.add leaf (test_record 1)
                   in
                   let child = child_split |> Either.find_left |> Option.get in
                   let* page = get_page child.id in
                   let leaf = page |> Aokvs.Page.Leaf.of_page |> Option.get in

                   let* child_split =
                     Aokvs.Page.Leaf.add leaf (test_record 2)
                   in
                   let child = child_split |> Either.find_left |> Option.get in
                   let* page = get_page child.id in
                   let leaf = page |> Aokvs.Page.Leaf.of_page |> Option.get in

                   Aokvs.Page.Leaf.add leaf (test_record 3)))
              |> fst |> Either.find_right |> Option.get
            in

            let left =
              Aokvs.Page.Pool.get_page pool left_child.id
              |> Aokvs.Page.Leaf.of_page |> Option.get
            in

            let right =
              Aokvs.Page.Pool.get_page pool right_child.id
              |> Aokvs.Page.Leaf.of_page |> Option.get
            in

            Alcotest.(
              check (list record_testable) "check records present in left leaf"
                (List.init 2 test_record)
                (List.of_seq @@ Aokvs.Page.Leaf.records left));

            Alcotest.(
              check (list record_testable) "check records present in right leaf"
                (List.of_seq @@ Seq.drop 2 @@ Seq.init 4 test_record)
                (List.of_seq @@ Aokvs.Page.Leaf.records right));

            ()))

  let test_cases =
    [
      test_allocate_empty;
      test_allocate_singleton;
      test_add;
      test_add_out_of_order;
      test_add_with_split;
    ]
end

module Node = struct
  let test_make =
    Alcotest.test_case "make a node with two leaves as children" `Quick
      (fun () ->
        with_pool (fun pool ->
            let allocator =
              Aokvs.Page.Allocator.(
                let* left =
                  Aokvs.Page.Leaf.singleton ("KEY_LEFT", "VALUE_LEFT")
                in
                let* right =
                  Aokvs.Page.Leaf.singleton ("KEY_RIGHT", "VALUE_RIGHT")
                in

                let* node = Aokvs.Page.Node.make left right in

                return (left, right, node))
            in

            let (left, right, node_child), _ =
              Aokvs.Page.Allocator.Unsafe.run ~pool ~next_free:0 allocator
            in

            let node =
              Aokvs.Page.Pool.get_page pool node_child.id
              |> Aokvs.Page.Node.of_page |> Option.get
            in

            Alcotest.(check int "count is 2" 2 (Aokvs.Page.Node.count node));

            Alcotest.(
              check (list child_testable)
                "left and right leaf are children of node" [ left; right ]
                (List.of_seq @@ Aokvs.Page.Node.children node));

            ()))

  let test_cases = [ test_make ]
end

let () =
  Alcotest.run "Aokvs.Page"
    [ ("Leaf", Leaf.test_cases); ("Node", Node.test_cases) ]
