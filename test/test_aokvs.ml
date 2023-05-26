(*
 * SPDX-FileCopyrightText: 2022 Tarides <contact@tarides.com>
 *
 * SPDX-License-Identifier: ISC
 *)

open Eio

let with_test_db f =
  Eio_main.run @@ fun env ->
  let path = Eio.Path.(Eio.Stdenv.cwd env / "test.db") in
  let return_value =
    Eio.Path.with_open_out ~create:(`If_missing 0o600) path (fun file ->
        let db = Aokvs.init file in

        f db)
  in
  Eio.Path.unlink path;
  return_value

let large_test_record i = ("KEY_" ^ string_of_int i, String.make 256 'x')

module Basic = struct
  let test_single_record =
    Alcotest.test_case "insert an retrieve a single record" `Quick (fun () ->
        with_test_db (fun db ->
            let key = "MY_KEY" in
            let value = "MY_VALUE" in

            Aokvs.update db key (Fun.const @@ Some value);

            Alcotest.(
              check (option string) "Can retrieve inserted value" (Some value)
              @@ Aokvs.find db key)))

  let test_multiple_leaves =
    Alcotest.test_case "insert more records than what fits in a leaf" `Quick
      (* This tests the split logic in `Page.Leaf.add`. *)
      (fun () ->
        with_test_db (fun db ->
            Seq.init 10 large_test_record
            |> Seq.iter (fun (key, value) ->
                   Aokvs.update db key (Fun.const @@ Some value));

            Seq.init 10 large_test_record
            |> Seq.iter (fun (key, value) ->
                   Alcotest.(
                     check (option string) "Can retrieve inserted value"
                       (Some value)
                     @@ Aokvs.find db key))))

  let test_multiple_nodes =
    Alcotest.test_case "insert more records than what fits in two leaves" `Quick
      (* This tests `Page.Node.replace_child` *)
      (fun () ->
        with_test_db (fun db ->
            (* TODO figure out heuristics to compute how many records of
               what size need to be inserted *)
            Seq.init 20 large_test_record
            |> Seq.iter (fun (key, value) ->
                   Aokvs.update db key (Fun.const @@ Some value));

            Seq.init 20 large_test_record
            |> Seq.iter (fun (key, value) ->
                   Alcotest.(
                     check (option string) "Can retrieve inserted value"
                       (Some value)
                     @@ Aokvs.find db key))))

  let test_node_split_child =
    Alcotest.test_case "insert more records than what fits in three leaves"
      `Quick (* This tests `Page.Node.replace_child` *) (fun () ->
        with_test_db (fun db ->
            (* TODO figure out heuristics to compute how many records of
               what size need to be inserted *)
            Seq.init 25 large_test_record
            |> Seq.iter (fun (key, value) ->
                   Aokvs.update db key (Fun.const @@ Some value));

            Seq.init 25 large_test_record
            |> Seq.iter (fun (key, value) ->
                   Alcotest.(
                     check (option string) "Can retrieve inserted value"
                       (Some value)
                     @@ Aokvs.find db key))))

  let test_cases =
    [
      test_single_record;
      test_multiple_leaves;
      test_multiple_nodes;
      test_node_split_child;
    ]
end

module Property_based = struct
  module Map = Map.Make (String)

  let gen_key = QCheck2.Gen.(small_nat |> map string_of_int)

  (* let gen_value = QCheck2.Gen.string_printable *)
  let gen_value = QCheck2.Gen.(small_nat |> map string_of_int)

  (* type event = Aokvs.key * Aokvs.value *)

  let gen_events =
    let open QCheck2.Gen in
    pair gen_key gen_value |> map (fun (k, v) -> (k, v)) |> list

  let test_insert_recrods =
    QCheck2.Test.make ~count:10 ~name:"Insert records" gen_events (fun events ->
        (* Init a databaes *)
        with_test_db (fun db ->
            (* Insert values into map and database *)
            let map =
              List.fold_left
                (fun map event ->
                  match event with
                  | key, value ->
                      let () =
                        Aokvs.update db key (Fun.const @@ Option.some value)
                      in
                      Map.update key (Fun.const @@ Option.some value) map)
                Map.empty events
            in

            (* Check that all values in map are in the database as well. *)
            (* Map.to_seq map *)
            (* |> Seq.for_all *)
            (*      (fun key value -> *)
            (*        match Aokvs.find db key with *)
            (*        | Some value_db -> *)
            (*            Alcotest.( *)
            (*              check string "value in DB matches expectation" value *)
            (*                value_db); *)
            (*            true *)
            (*        | None -> false) *)
            (*      map; *)
            map |> Map.to_seq
            |> Seq.for_all (fun (key, value) ->
                   match Aokvs.find db key with
                   | Some value_db -> value = value_db
                   | None ->
                       traceln "FAILURE: key-value (%s - %s) not found in DB"
                         key value;

                       traceln "FAILURE: events: %a"
                         Fmt.(
                           list ~sep:semi @@ parens
                           @@ pair ~sep:comma string string)
                         events;

                       false)))

  let test_cases = [ QCheck_alcotest.to_alcotest test_insert_recrods ]
end

let () =
  Alcotest.run "Aokvs"
    [
      ("Basic", Basic.test_cases); ("Property-based", Property_based.test_cases);
    ]
