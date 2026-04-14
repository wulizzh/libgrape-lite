#!/usr/bin/env python3

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Convert ljkdataset-style vertex/edge files into a METIS graph, "
            "optionally run gpmetis, and export explicit vertex-to-partition "
            "assignments for libgrape-lite-privacy."
        )
    )
    parser.add_argument("--vfile", required=True, help="Path to the vertex file.")
    parser.add_argument("--efile", required=True, help="Path to the edge file.")
    parser.add_argument(
        "--parts", type=int, required=True, help="Number of target partitions."
    )
    parser.add_argument(
        "--output-prefix",
        required=True,
        help=(
            "Prefix for generated files, for example "
            "/tmp/web-google/metis/web-google_8w"
        ),
    )
    parser.add_argument(
        "--gpmetis-bin",
        default="gpmetis",
        help="Path to the gpmetis executable. Default: gpmetis",
    )
    parser.add_argument(
        "--sort-bin",
        default="sort",
        help="Path to the sort executable. Default: sort",
    )
    parser.add_argument(
        "--sort-tmpdir",
        default="",
        help="Optional temporary directory forwarded to sort -T.",
    )
    parser.add_argument(
        "--existing-part-file",
        default="",
        help=(
            "Existing METIS part file to convert directly. If set, gpmetis "
            "is not invoked."
        ),
    )
    parser.add_argument(
        "--prepare-only",
        action="store_true",
        help="Only generate the METIS graph and vertex-order files.",
    )
    parser.add_argument(
        "--keep-temp",
        action="store_true",
        help="Keep intermediate unsorted and sorted edge lists.",
    )
    return parser.parse_args()


def data_lines(path: Path):
    with path.open("r", encoding="utf-8") as stream:
        for line_no, raw_line in enumerate(stream, start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            yield line_no, line


def load_vertex_ids(vfile: Path):
    vertex_ids = []
    sequential = True
    start_id = None
    previous_id = None

    for line_no, line in data_lines(vfile):
        parts = line.split()
        if len(parts) < 1:
            raise ValueError(f"Invalid vertex line {line_no}: {line}")
        try:
            vertex_id = int(parts[0])
        except ValueError as exc:
            raise ValueError(
                f"Vertex id on line {line_no} is not an integer: {parts[0]}"
            ) from exc
        vertex_ids.append(vertex_id)
        if start_id is None:
            start_id = vertex_id
        elif previous_id is None or vertex_id != previous_id + 1:
            sequential = False
        previous_id = vertex_id

    if not vertex_ids:
        raise ValueError(f"No vertices parsed from {vfile}")

    if sequential and start_id in (0, 1):
        base = start_id

        def oid_to_metis(oid):
            metis_id = oid - base + 1
            if metis_id < 1 or metis_id > len(vertex_ids):
                raise KeyError(oid)
            return metis_id

        mapping_mode = f"sequential-{base}-based"
        mapping = None
    else:
        mapping = {vertex_id: index for index, vertex_id in enumerate(vertex_ids, start=1)}

        def oid_to_metis(oid):
            return mapping[oid]

        mapping_mode = "explicit-map"

    return vertex_ids, oid_to_metis, mapping_mode


def write_vertex_order(vertex_ids, output_path: Path):
    with output_path.open("w", encoding="utf-8") as stream:
        for index, vertex_id in enumerate(vertex_ids, start=1):
            stream.write(f"{index}\t{vertex_id}\n")


def build_edge_lists(efile: Path, oid_to_metis, unsorted_path: Path):
    raw_edges = 0
    directed_entries = 0

    with unsorted_path.open("w", encoding="utf-8") as out:
        for line_no, line in data_lines(efile):
            parts = line.split()
            if len(parts) < 2:
                raise ValueError(f"Invalid edge line {line_no}: {line}")
            try:
                src = int(parts[0])
                dst = int(parts[1])
            except ValueError as exc:
                raise ValueError(
                    f"Edge endpoints on line {line_no} are not integers: {line}"
                ) from exc

            try:
                src_metis = oid_to_metis(src)
                dst_metis = oid_to_metis(dst)
            except KeyError as exc:
                raise ValueError(
                    f"Edge line {line_no} references a vertex absent from the vfile: "
                    f"{exc.args[0]}"
                ) from exc

            raw_edges += 1
            if src_metis == dst_metis:
                continue

            out.write(f"{src_metis}\t{dst_metis}\n")
            out.write(f"{dst_metis}\t{src_metis}\n")
            directed_entries += 2

    return raw_edges, directed_entries


def sort_edges(sort_bin: str, unsorted_path: Path, sorted_path: Path, sort_tmpdir: str):
    env = os.environ.copy()
    env["LC_ALL"] = "C"
    cmd = [sort_bin]
    if sort_tmpdir:
        cmd.extend(["-T", sort_tmpdir])
    cmd.extend(["-n", "-k1,1", "-k2,2", str(unsorted_path), "-o", str(sorted_path)])
    subprocess.run(cmd, check=True, env=env)


def build_metis_graph(sorted_path: Path, graph_path: Path, body_path: Path, vertex_count: int):
    undirected_edges = 0
    current_src = None
    next_src_to_write = 1
    current_neighbors = []
    last_dst = None

    def flush_vertex(body_stream, src, neighbors):
        body_stream.write(" ".join(neighbors))
        body_stream.write("\n")

    with sorted_path.open("r", encoding="utf-8") as src_stream, body_path.open(
        "w", encoding="utf-8"
    ) as body_stream:
        for raw_line in src_stream:
            line = raw_line.strip()
            if not line:
                continue
            src_text, dst_text = line.split()
            src = int(src_text)
            dst = int(dst_text)

            if current_src is None:
                while next_src_to_write < src:
                    body_stream.write("\n")
                    next_src_to_write += 1
                current_src = src
                current_neighbors = []
                last_dst = None
            elif src != current_src:
                flush_vertex(body_stream, current_src, current_neighbors)
                next_src_to_write = current_src + 1
                while next_src_to_write < src:
                    body_stream.write("\n")
                    next_src_to_write += 1
                current_src = src
                current_neighbors = []
                last_dst = None

            if dst == last_dst:
                continue

            current_neighbors.append(str(dst))
            if src < dst:
                undirected_edges += 1
            last_dst = dst

        if current_src is None:
            for _ in range(vertex_count):
                body_stream.write("\n")
        else:
            flush_vertex(body_stream, current_src, current_neighbors)
            next_src_to_write = current_src + 1
            while next_src_to_write <= vertex_count:
                body_stream.write("\n")
                next_src_to_write += 1

    with graph_path.open("w", encoding="utf-8") as graph_stream:
        graph_stream.write(f"{vertex_count} {undirected_edges}\n")
        with body_path.open("r", encoding="utf-8") as body_stream:
            shutil.copyfileobj(body_stream, graph_stream)

    return undirected_edges


def run_gpmetis(gpmetis_bin: str, graph_path: Path, parts: int):
    subprocess.run([gpmetis_bin, str(graph_path), str(parts)], check=True)
    return Path(f"{graph_path}.part.{parts}")


def convert_part_file(vertex_ids, part_path: Path, assignment_path: Path):
    with part_path.open("r", encoding="utf-8") as part_stream, assignment_path.open(
        "w", encoding="utf-8"
    ) as out_stream:
        for index, vertex_id in enumerate(vertex_ids, start=1):
            line = part_stream.readline()
            if not line:
                raise ValueError(
                    f"Part file ended early at vertex index {index} while converting {part_path}"
                )
            try:
                partition_id = int(line.strip())
            except ValueError as exc:
                raise ValueError(
                    f"Invalid partition id at vertex index {index}: {line.strip()}"
                ) from exc
            out_stream.write(f"{vertex_id}\t{partition_id}\n")

        extra_line = part_stream.readline()
        if extra_line:
            raise ValueError(
                f"Part file {part_path} contains more lines than vertices in the vfile."
            )


def main():
    args = parse_args()

    if args.parts <= 0:
        raise ValueError("--parts must be positive.")

    output_prefix = Path(args.output_prefix)
    output_prefix.parent.mkdir(parents=True, exist_ok=True)

    vfile = Path(args.vfile)
    efile = Path(args.efile)
    if not vfile.exists():
        raise FileNotFoundError(f"Vertex file does not exist: {vfile}")
    if not efile.exists():
        raise FileNotFoundError(f"Edge file does not exist: {efile}")

    vertex_ids, oid_to_metis, mapping_mode = load_vertex_ids(vfile)
    vertex_count = len(vertex_ids)

    vertex_order_path = Path(f"{output_prefix}.metis.vertex_order.tsv")
    unsorted_path = Path(f"{output_prefix}.metis.edges.unsorted.tsv")
    sorted_path = Path(f"{output_prefix}.metis.edges.sorted.tsv")
    body_path = Path(f"{output_prefix}.metis.body")
    graph_path = Path(f"{output_prefix}.metis.graph")
    assignment_path = Path(f"{output_prefix}.metis.assignments.part{args.parts}.tsv")

    print(f"[METIS] parsed {vertex_count} vertices from {vfile}")
    print(f"[METIS] vertex-id mapping mode: {mapping_mode}")

    write_vertex_order(vertex_ids, vertex_order_path)
    raw_edges, directed_entries = build_edge_lists(efile, oid_to_metis, unsorted_path)
    print(f"[METIS] parsed {raw_edges} raw edges from {efile}")
    print(f"[METIS] wrote {directed_entries} directed adjacency entries before sort")

    sort_edges(args.sort_bin, unsorted_path, sorted_path, args.sort_tmpdir)
    undirected_edges = build_metis_graph(sorted_path, graph_path, body_path, vertex_count)
    print(f"[METIS] wrote graph file: {graph_path}")
    print(f"[METIS] deduplicated undirected edge count: {undirected_edges}")

    if not args.keep_temp:
        body_path.unlink(missing_ok=True)

    if args.prepare_only:
        print("[METIS] prepare-only mode enabled; skipped gpmetis and assignment export")
        if not args.keep_temp:
            unsorted_path.unlink(missing_ok=True)
            sorted_path.unlink(missing_ok=True)
        return 0

    if args.existing_part_file:
        part_path = Path(args.existing_part_file)
        if not part_path.exists():
            raise FileNotFoundError(f"Existing part file does not exist: {part_path}")
        print(f"[METIS] using existing part file: {part_path}")
    else:
        part_path = run_gpmetis(args.gpmetis_bin, graph_path, args.parts)
        print(f"[METIS] gpmetis output: {part_path}")

    convert_part_file(vertex_ids, part_path, assignment_path)
    print(f"[METIS] explicit assignment file: {assignment_path}")

    if not args.keep_temp:
        unsorted_path.unlink(missing_ok=True)
        sorted_path.unlink(missing_ok=True)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # pylint: disable=broad-except
        print(f"[METIS][ERROR] {exc}", file=sys.stderr)
        sys.exit(1)
