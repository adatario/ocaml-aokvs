(*
 * SPDX-FileCopyrightText: 2022 Tarides <contact@tarides.com>
 *
 * SPDX-License-Identifier: ISC
 *)

module Kv = struct
  module Map = Map.Make (String)

  let gen_key = QCheck2.Gen.(int |> map string_of_int)
  let gen_value = QCheck2.Gen.string_printable

  type event = Set of Omdb.key * Omdb.value

  let gen_events =
    let open QCheck2.Gen in
    pair gen_key gen_value |> map (fun (k, v) -> Set (k, v)) |> list

  let test_insert_recrods =
    QCheck2.Test.make ~count:10 ~name:"Insert records" gen_events (fun events ->
        (* Init a databaes *)
        let db = Omdb.init () in

        (* Insert values into map and database *)
        let map =
          List.fold_left
            (fun map event ->
              match event with
              | Set (key, value) ->
                  let () = Omdb.set db key value in
                  Map.add key value map)
            Map.empty events
        in

        (* Check that all values in map are in the database as well. *)
        Map.iter
          (fun key value ->
            match Omdb.find db key with
            | Some value_db ->
                Alcotest.(
                  check string "value in DB matches expectation" value value_db)
            | None -> Alcotest.fail "value missing in DB")
          map;
        true)

  let test_cases = [ QCheck_alcotest.to_alcotest test_insert_recrods ]
end

let () = Alcotest.run "Omdb" [ ("Kv", Kv.test_cases) ]
