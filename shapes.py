"""READ-ONLY shape dumper. Writes nothing, deletes nothing.

rawcheck.py found keys the API sends that nothing captures. Before writing a
parser for them we need their exact JSON shape (list? dict? which sub-keys?).
This prints a few real examples of each, pretty-printed.

    python shapes.py [db_path] [examples_per_key]
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db

# table -> keys whose shape we need in order to write an extractor
WANT = {
    "colleges_directory": [
        "fees",                 # <- top_course_fees reads this and gets nothing
        "placement",            # avg / highest salary
        "placement_percentage",
        "rankingData",
        "stream_ranking",
        "facilities",
        "reviewsData",
        "availableTabs",
        "major_stream_rating",
        "tagline",
        "view_all_course",
        "logo",
        "cover",
    ],
    "sa_programs": ["reviews"],
    "courses": ["fees", "avg_salary", "topics_covered"],
    "offerings": ["avg_salary", "description", "topics_covered"],
}


def dump(conn, table, keys, k_examples):
    try:
        rows = conn.execute(
            f"SELECT raw_json FROM {table} WHERE raw_json IS NOT NULL AND raw_json<>'' "
            f"LIMIT 400").fetchall()
    except Exception as e:
        print(f"\n{table}: cannot read ({e})")
        return
    objs = []
    for (rj,) in rows:
        try:
            o = json.loads(rj)
            if isinstance(o, dict):
                objs.append(o)
        except Exception:
            continue
    if not objs:
        print(f"\n{table}: no usable raw_json")
        return

    print(f"\n{'='*78}\n{table}  ({len(objs)} payloads scanned)\n{'='*78}")
    for key in keys:
        seen = 0
        types = {}
        print(f"\n--- {key} ---")
        for o in objs:
            if key not in o:
                continue
            v = o[key]
            types[type(v).__name__] = types.get(type(v).__name__, 0) + 1
            empty = v in (None, "", [], {}, 0)
            if empty or seen >= k_examples:
                continue
            seen += 1
            try:
                txt = json.dumps(v, ensure_ascii=False, indent=2)
            except Exception:
                txt = repr(v)
            if len(txt) > 900:
                txt = txt[:900] + "\n  … (truncated)"
            print(f"  example {seen}: {txt}")
        if not types:
            print("  (key never present)")
        else:
            print(f"  types seen: {types}")
            if seen == 0:
                print("  (present, but every value was empty)")


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else db.DB_PATH
    k = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    print(f"DB: {path}")
    with db.connect(path) as conn:
        for t, keys in WANT.items():
            dump(conn, t, keys, k)


if __name__ == "__main__":
    main()
