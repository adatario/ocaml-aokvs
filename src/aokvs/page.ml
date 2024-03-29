(*
 * SPDX-FileCopyrightText: 2022 Tarides <contact@tarides.com>
 *
 * SPDX-License-Identifier: ISC
 *)

open Eio
open Record

let page_size = 4096

type key = string
type value = string
type record = key * value

(* Phantom types *)

type any_kind
type leaf_kind
type node_kind
type 'a page_kind = Bigstringaf.t

(** Page *)

type page = any_kind page_kind

(* Identifier *)

type id = int

let id_equal = Int.equal
let pp_id = Fmt.(styled `Cyan @@ int)

(* Page Pool *)

module Pool = struct
  type t = Eio.File.rw

  let init file =
    let file = (file :> Eio.File.rw) in
    file

  let offset id = Optint.Int63.of_int @@ (page_size * id)

  let get_page t id =
    let buf = Cstruct.create page_size in
    (try Eio.File.pread_exact t ~file_offset:(offset id) [ buf ]
     with End_of_file -> ());
    Cstruct.to_bigarray buf

  let write_page t id page =
    traceln "Pool.write_page ~id:%d" id;
    let buf = Cstruct.of_bigarray page in
    Eio.File.pwrite_all t ~file_offset:(offset id) [ buf ]
end

(* Allocator *)

module Allocator = struct
  module PageTable = Hashtbl.Make (struct
    type t = id

    let equal = id_equal
    let hash = Hashtbl.hash
  end)

  type allocator = {
    next_free : id;
    pool : Pool.t;
    allocated : page PageTable.t;
  }

  type 'a t = allocator -> 'a * allocator

  (* Combinators *)

  let return v allocator = (v, allocator)

  let map a f allocator =
    let v, allocator' = a allocator in
    (f v, allocator')

  let ( let+ ) = map
  let ( >>| ) = map

  let bind a f allocator =
    let a_v, a_allocator = a allocator in
    (f a_v) a_allocator

  let ( let* ) = bind
  let ( >>= ) = bind

  (* page allocation primitive *)

  let alloc_page () : (id * page) t =
    traceln "alloc_page";
    fun allocator ->
      let id = allocator.next_free in
      let page = Bigstringaf.create page_size in
      PageTable.add allocator.allocated id page;
      ((id, page), { allocator with next_free = id + 1 })

  (* Utilities *)

  let get_page id : page t =
   fun allocator ->
    match PageTable.find_opt allocator.allocated id with
    (* If page has been allocated during by this allocator get directly from memory *)
    | Some page -> (page, allocator)
    (* else get it from the pool *)
    | None ->
        let page = Pool.get_page allocator.pool id in
        (page, allocator)

  module Unsafe = struct
    let run ~pool ~next_free t =
      let allocated = PageTable.create 50 in
      let v, { next_free; _ } = t { next_free; pool; allocated } in

      (* Write all freshly allocated pages *)
      PageTable.iter (fun id page -> Pool.write_page pool id page) allocated;

      (v, next_free)
  end
end

(* Helper Types *)

(* TODO this should probably be called child_ref *)
type child = { id : id; min_key : key }
type split = (child, child * child) Either.t

(* Page Types *)

module Leaf = struct
  open Allocator

  type t = leaf_kind page_kind

  let magic_number = 0

  let of_page page =
    if Bigstringaf.get_int16_le page 0 = magic_number then Some page else None

  (* Header *)

  module Header = struct
    [%%cstruct
    type t = {
      magic_number : uint16_t;
      count : uint16_t;
      max_offset : uint16_t;
      padding : uint16_t; (* padding to make uint64s aligned *)
      left : uint64_t;
      right : uint64_t;
    }
    [@@little_endian]]
  end

  let header t = Cstruct.of_bigarray ~off:0 ~len:Header.sizeof_t t
  let max_offset t = Header.get_t_max_offset @@ header t
  let count t = Header.get_t_count @@ header t

  (* Record entry *)

  module Record_entry = struct
    [%%cstruct
    type t = {
      key_offset : uint16_t;
      key_length : uint16_t;
      value_offset : uint16_t;
      value_length : uint16_t;
    }
    [@@little_endian]]
  end

  let record_entry t i =
    Cstruct.of_bigarray
      ~off:(Header.sizeof_t + (i * Record_entry.sizeof_t))
      ~len:Record_entry.sizeof_t t

  let record_entries t = Seq.init (count t) (record_entry t)

  (* Records *)

  let write_record ~max_offset ~record_entry page (key, value) =
    (* write key *)
    let key_length = String.length key in
    let key_offset = max_offset + key_length in
    Bigstringaf.blit_from_string key ~src_off:0 page
      ~dst_off:(page_size - key_offset) ~len:key_length;

    (* write value *)
    let value_length = String.length value in
    let value_offset = key_offset + String.length value in
    Bigstringaf.blit_from_string value ~src_off:0 page
      ~dst_off:(page_size - value_offset) ~len:value_length;

    (* write record entry *)
    Record_entry.set_t_key_offset record_entry key_offset;
    Record_entry.set_t_key_length record_entry key_length;
    Record_entry.set_t_value_offset record_entry value_offset;
    Record_entry.set_t_value_length record_entry value_length;

    (* return new max_offset *)
    value_offset

  let get_from_offset ~off ~len t =
    Bigstringaf.substring ~off:(page_size - off) ~len t

  let get_key t record_entry =
    get_from_offset
      ~off:(Record_entry.get_t_key_offset record_entry)
      ~len:(Record_entry.get_t_key_length record_entry)
      t

  let get_value t record_entry =
    get_from_offset
      ~off:(Record_entry.get_t_value_offset record_entry)
      ~len:(Record_entry.get_t_value_length record_entry)
      t

  let compare_record_entry_key t a b =
    String.compare (get_key t a) (get_key t b)

  (* Accessors *)

  let find_position t key =
    record_entries t
    |> Seq.mapi (fun i record_entry -> (i, record_entry))
    |> Seq.find_map (fun (i, record_entry) ->
           let r_key = get_key t record_entry in
           if r_key = key then Some i else None)

  let get_record t pos =
    let record_entry = record_entry t pos in
    (get_key t record_entry, get_value t record_entry)

  let records t = Seq.init (count t) (get_record t)
  let min_key t = get_key t @@ record_entry t 0

  let free_space t =
    let max_offset = Header.get_t_max_offset @@ header t in
    let count = count t in
    page_size - Header.sizeof_t - (count * Record_entry.sizeof_t) - max_offset

  let has_space t (key, value) =
    let required_space =
      String.length key + String.length value + Record_entry.sizeof_t
    in
    free_space t >= required_space

  let pp ppf t =
    Fmt.(pf ppf "%a" @@ brackets @@ seq ~sep:semi pp_record)
      (Seq.map (fun record_entry ->
           (get_key t record_entry, get_value t record_entry))
      @@ record_entries t)

  (* Allocation *)

  (* Calees of `alloc_records` must make sure that records fit into one page. *)
  let alloc_records ?(left = Int64.of_int 0) ?(right = Int64.of_int 0) records =
    let* id, t = alloc_page () in

    let header = header t in
    Header.set_t_magic_number header magic_number;
    Header.set_t_left header (Int64.of_int 0);
    Header.set_t_right header (Int64.of_int 0);

    let i, max_offset =
      Seq.fold_left
        (fun (i, max_offset) record ->
          let record_entry = record_entry t i in
          let max_offset = write_record ~max_offset ~record_entry t record in
          (i + 1, max_offset))
        (0, 0) records
    in

    Header.set_t_count header i;
    Header.set_t_max_offset header max_offset;
    Header.set_t_left header left;
    Header.set_t_right header right;

    return (id, t)

  let empty =
    let* id, _page = alloc_records Seq.empty in
    return id

  let singleton record =
    let* id, _page = alloc_records @@ Seq.return record in

    return { id; min_key = fst record }

  let add t (key, value) =
    if has_space t (key, value) then (
      (* We don't use the alloc_records, instead we copy all the
         records as is from the old page and append the new record. This
         should be more efficient as we do a single larger `blit`
         operation instead of many smaller ones. A.k.a premature
         optimiazation... *)

      (* the old header *)
      let header_old = header t in

      (* allocate a new page *)
      let* id, t_new = alloc_page () in

      (* Set the header *)
      let header_new = header t_new in
      Header.set_t_magic_number header_new magic_number;
      Header.set_t_count header_new (count t + 1);
      Header.set_t_left header_new (Header.get_t_left header_old);
      Header.set_t_right header_new (Header.get_t_right header_old);

      (* Copy existing records *)
      Bigstringaf.blit t
        ~src_off:(page_size - max_offset t)
        t_new
        ~dst_off:(page_size - max_offset t)
        ~len:(max_offset t);

      (* copy existing record entries and write new one for new record *)
      let _i, max_offset =
        Seq.sorted_merge
          (fun a b ->
            match (a, b) with
            | Either.Left a, Either.Left b -> compare_record_entry_key t a b
            | Either.Left a, Either.Right (b_key, _) ->
                String.compare (get_key t a) b_key
            | Either.Right (a_key, _), Either.Left b ->
                String.compare a_key (get_key t b)
            | Either.Right _, Either.Right _ ->
                (* there is only a single right entry *)
                0)
          (record_entries t |> Seq.map Either.left)
          (Either.right (key, value) |> Seq.return)
        |> Seq.fold_left
             (fun (i, max_offset) re_either ->
               let new' = record_entry t_new i in
               let open Record_entry in
               match re_either with
               | Either.Left old ->
                   set_t_key_offset new' (get_t_key_offset old);
                   set_t_key_length new' (get_t_key_length old);
                   set_t_value_offset new' (get_t_value_offset old);
                   set_t_value_length new' (get_t_value_length old);

                   (i + 1, max_offset)
               | Either.Right record ->
                   let max_offset =
                     write_record ~max_offset ~record_entry:new' t_new record
                   in
                   (i + 1, max_offset))
             (0, max_offset t)
      in

      (* store the new max_offset *)
      Header.set_t_max_offset header_new max_offset;

      (* return the id and the minimal key *)
      return @@ Either.left { id; min_key = min_key t_new })
    else
      (* leaf needs to be split in two *)

      (* the total required space (minus headers) *)
      let total_required_space =
        max_offset t
        + ((count t + 1) * Record_entry.sizeof_t)
        + String.length key + String.length value
      in

      let threshold = total_required_space / 2 in

      let records_left, records_right =
        Seq.sorted_merge compare_record (records t) (Seq.return (key, value))
        |> Seq.map (fun record -> record)
        |> Seq.scan
             (fun (used_space, _) record ->
               let required_space =
                 Record_entry.sizeof_t + String.length key + String.length value
               in

               if used_space + required_space <= threshold then
                 (used_space + required_space, Option.some @@ Either.left record)
               else
                 ( used_space + required_space,
                   Option.some @@ Either.right record ))
             (0, None)
        |> Seq.filter_map snd |> Seq.memoize |> Seq.partition_map Fun.id
      in

      (* header of the old page *)
      let header_old = header t in

      (* Allocate left child with left pointer to the old left. *)
      let* id_left, left =
        alloc_records ~left:(Header.get_t_left header_old) records_left
      in

      (* Allocate right child with right pointer to the old rigth. *)
      let* id_right, right =
        alloc_records ~right:(Header.get_t_right header_old) records_right
      in

      (* Set the right pointer of the left child to the right child. *)
      Header.set_t_right (header left) (Int64.of_int id_right);

      (* Set the left pointer of the right child to the left child. *)
      Header.set_t_left (header right) (Int64.of_int id_left);

      let child_left = { id = id_left; min_key = min_key left } in
      let child_right = { id = id_right; min_key = min_key right } in

      return @@ Either.right (child_left, child_right)

  let replace_value t ~pos value =
    (* TODO: this does not check the new size of the value *)
    let header = header t in
    let* id, leaf =
      records t
      |> Seq.mapi (fun i (r_key, r_value) ->
             if i = pos then (r_key, value) else (r_key, r_value))
      |> alloc_records ~left:(Header.get_t_left header)
           ~right:(Header.get_t_right header)
    in
    return { id; min_key = min_key leaf }
end

module Node = struct
  type t = node_kind page_kind

  let magic_number = 1

  let of_page page =
    if Bigstringaf.get_int16_le page 0 = magic_number then Some page else None

  (* Header *)

  module Header = struct
    [%%cstruct
    type t = {
      magic_number : uint16_t;
      count : uint16_t;
      max_offset : uint16_t;
    }
    [@@little_endian]]
  end

  let header t = Cstruct.of_bigarray ~off:0 ~len:Header.sizeof_t t
  let count t = Header.get_t_count @@ header t

  (* Child entry *)

  module Child_entry = struct
    [%%cstruct
    type t = {
      key_offset : uint16_t;
      key_length : uint16_t;
      page_id : uint64_t;
    }
    [@@little_endian]]
  end

  let child_entry t i =
    Cstruct.of_bigarray
      ~off:(Header.sizeof_t + (i * Child_entry.sizeof_t))
      ~len:Child_entry.sizeof_t t

  let child_entries t = Seq.init (count t) (child_entry t)

  let write_key ~max_offset ~key ~child_entry page =
    (* write key *)
    let key_length = String.length key in
    let key_offset = max_offset + key_length in
    Bigstringaf.blit_from_string key ~src_off:0 page
      ~dst_off:(page_size - key_offset) ~len:key_length;

    (* write child entry *)
    Child_entry.set_t_key_offset child_entry key_offset;
    Child_entry.set_t_key_length child_entry key_length;

    (* return new max_offset *)
    key_offset

  let get_from_offset ~off ~len t =
    Bigstringaf.substring ~off:(page_size - off) ~len t

  let get_key t child_entry =
    get_from_offset
      ~off:(Child_entry.get_t_key_offset child_entry)
      ~len:(Child_entry.get_t_key_length child_entry)
      t

  (* Accessors *)

  let child_id t i =
    Int64.to_int @@ Child_entry.get_t_page_id @@ child_entry t i

  let children t =
    child_entries t
    |> Seq.map (fun child_entry ->
           {
             id = Int64.to_int @@ Child_entry.get_t_page_id child_entry;
             min_key = get_key t child_entry;
           })

  let search t key =
    (* Sequence of pivots of the node. The lenght of the sequence is
       equal to the lenght of children. The last pivot is None. *)
    let pivots =
      child_entries t
      (* the first min_key is not used as pivot *)
      |> Seq.drop 1
      |> Seq.map (fun child_entry -> Option.some @@ get_key t child_entry)
      (* the last child does not need a pivot *)
      |> fun pivots -> Seq.(append pivots (return None))
    in

    pivots
    |> Seq.mapi (fun pos pivot -> (pos, pivot))
    |> Seq.find_map (fun (pos, pivot) ->
           match pivot with
           | Some pivot -> if key < pivot then Some pos else None
           | None -> Some pos)
    |> Option.value ~default:0
    |> fun pos -> (pos, child_id t pos)

  let min_key t = child_entry t 0 |> get_key t

  let free_space t =
    let max_offset = Header.get_t_max_offset @@ header t in
    let count = count t in
    page_size - Header.sizeof_t - (count * Child_entry.sizeof_t) - max_offset

  (* Allocators *)

  open Allocator

  let alloc_node children =
    let* id, t = alloc_page () in

    (* Set the header *)
    let header = header t in
    Header.set_t_magic_number header magic_number;

    (* write child entries *)
    let count, max_offset =
      Seq.fold_left
        (fun (i, max_offset) child ->
          let child_entry = child_entry t i in

          Child_entry.set_t_page_id child_entry (Int64.of_int child.id);

          let max_offset =
            write_key ~max_offset ~key:child.min_key ~child_entry t
          in

          (i + 1, max_offset))
        (0, 0) children
    in

    (* set the count and max_offset in header *)
    Header.set_t_count header count;
    Header.set_t_max_offset header max_offset;

    let min_key = min_key t in

    return { id; min_key }

  let make left right = alloc_node @@ List.to_seq [ left; right ]

  let replace_child t ~pos new_child =
    (* TODO split or merge if necessary *)
    children t
    |> Seq.mapi (fun i child -> if i = pos then new_child else child)
    |> alloc_node

  let split_child t ~pos left_child right_child =
    let key_lenght_of_child_to_replace =
      Child_entry.get_t_key_length @@ child_entry t pos
    in

    let required_space =
      (* size of new keys that need to be stored *)
      String.length left_child.min_key
      + String.length right_child.min_key
      (* minus the size of the key to be replaced *)
      - key_lenght_of_child_to_replace
      (* plus one additional child_entry *)
      + Child_entry.sizeof_t
    in

    let has_space = free_space t >= required_space in

    if has_space then
      children t
      |> Seq.mapi (fun i child ->
             if i = pos then List.to_seq [ left_child; right_child ]
             else Seq.return child)
      |> Seq.concat |> alloc_node >>| Either.left
    else failwith "TODO split_child causes a node split"

  let pp ppf t =
    Fmt.(
      pf ppf "%a" @@ brackets @@ seq ~sep:semi
      @@ record
           [
             field "id"
               (fun child_entry ->
                 Int64.to_int @@ Child_entry.get_t_page_id child_entry)
               pp_id;
             field "min_key" (fun child_entry -> get_key t child_entry) pp_key;
           ])
      (child_entries t)
end
