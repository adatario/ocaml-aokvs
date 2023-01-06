(*
 * SPDX-FileCopyrightText: 2022 Tarides <contact@tarides.com>
 *
 * SPDX-License-Identifier: ISC
 *)

open Eio
open Bigarray

module Random = struct
  module Map = Map.Make (String)

  let gen_key = QCheck2.Gen.(small_nat |> map string_of_int)

  (* let gen_value = QCheck2.Gen.string_printable *)
  let gen_value = QCheck2.Gen.(small_nat |> map string_of_int)

  (* type event = Omdb.key * Omdb.value *)

  let gen_events =
    let open QCheck2.Gen in
    pair gen_key gen_value |> map (fun (k, v) -> (k, v)) |> list

  let test_insert_recrods =
    QCheck2.Test.make ~count:10 ~name:"Insert records" gen_events (fun events ->
        (* Init a databaes *)
        let db = Omdb.init (Array2.create char c_layout 1024 4096) in

        (* Insert values into map and database *)
        let map =
          List.fold_left
            (fun map event ->
              match event with
              | key, value ->
                  let () =
                    Omdb.update db key (Fun.const @@ Option.some value)
                  in
                  Map.update key (Fun.const @@ Option.some value) map)
            Map.empty events
        in

        (* Check that all values in map are in the database as well. *)
        (* Map.to_seq map *)
        (* |> Seq.for_all *)
        (*      (fun key value -> *)
        (*        match Omdb.find db key with *)
        (*        | Some value_db -> *)
        (*            Alcotest.( *)
        (*              check string "value in DB matches expectation" value *)
        (*                value_db); *)
        (*            true *)
        (*        | None -> false) *)
        (*      map; *)
        map |> Map.to_seq
        |> Seq.for_all (fun (key, value) ->
               match Omdb.find db key with
               | Some value_db -> value = value_db
               | None ->
                   traceln "FAILURE: key-value (%s - %s) not found in DB" key
                     value;

                   traceln "FAILURE: events: %a"
                     Fmt.(
                       list ~sep:semi @@ parens @@ pair ~sep:comma string string)
                     events;

                   false))

  let test_cases = [ QCheck_alcotest.to_alcotest test_insert_recrods ]
end

let () = Alcotest.run "Omdb" [ ("Random", Random.test_cases) ]
