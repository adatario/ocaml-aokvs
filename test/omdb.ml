(*
 * SPDX-FileCopyrightText: 2022 Tarides <contact@tarides.com>
 *
 * SPDX-License-Identifier: ISC
 *)

open Eio

module Kv = struct
  module Map = Map.Make (Int)

  let gen_key = QCheck2.Gen.int
  let gen_value = QCheck2.Gen.string_printable

  type event =
    | Set of Omdb.key * Omdb.value
    | Find of Omdb.key * Omdb.value option

  let pp_key = Fmt.int
  let pp_value = Fmt.string

  let pp_event ppf = function
    | Set (key, value) ->
        Fmt.pf ppf "@[Insert %a → %a@]" pp_key key pp_value value
    | Find (key, value_opt) ->
        Fmt.pf ppf "@[Find %a → %a@]" pp_key key
          Fmt.(option ~none:(styled `Faint @@ any "None") pp_value)
          value_opt

  let gen_events =
    let open QCheck2.Gen in
    pair gen_key gen_value |> map (fun (k, v) -> Set (k, v)) |> list

  let test_insert_recrods =
    QCheck2.Test.make ~count:10 ~name:"Insert records" gen_events (fun events ->
        (* Insert values into map and database *)
        let db, map =
          List.fold_left
            (fun (db, map) event ->
              match event with
              | Set (key, value) ->
                  (Omdb.set key value db, Map.add key value map)
              | Find _ -> (db, map))
            (Omdb.init (), Map.empty)
            events
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
