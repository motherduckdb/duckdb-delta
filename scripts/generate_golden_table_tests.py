

def generate_test_header(name, filename):
    return f"""# name: test/sql/golden_tests/{filename}
# description: generated {name} golden table tests
# group: [delta_kernel_rs]

require parquet

require delta

require-env GOLDEN_TABLES_PATH

# WARNING: this file is generated automatically, do not edit manually (see scripts/generate_golden_table_tests.py)
"""

def generate_golden_test(test_name, thing):
    if thing == "latest_snapshot_test":
        print(f"generating: {test_name}, {thing}")
        col_count_str = "T"
        return f"""
######## {test_name} ########

query {col_count_str} rowsort res-{test_name}
from delta_scan('${{GOLDEN_TABLES_PATH}}/{test_name}/delta')

query {col_count_str} rowsort {test_name}
from parquet_scan('${{GOLDEN_TABLES_PATH}}/{test_name}/expected/**/*.parquet')
"""
    else:
        print(f"skipping: {test_name}, unknown test type: {thing}")
        return ""

def skip_test(test_name, thing):
    print(f"skipping: {test_name}, {thing}")
    return ""

def generate_negative_test(test_name):
    print(f"generating negative test: {test_name}")
    return f"""
######## {test_name} ########

statement error
from delta_scan('${{GOLDEN_TABLES_PATH}}/{test_name}/delta')
----
IO Error: DeltaKernel

"""
def write_base_tests():
    name = "generated"
    file_name = "generated.test"
    file = f"test/sql/golden_tests/{name}.test"
    result = generate_test_header(name, file_name)

    # Broken cols: array_of_structs, map_of_rows

    result += generate_golden_test("124-decimal-decode-bug", "latest_snapshot_test")
    result += generate_golden_test("125-iterator-bug", "latest_snapshot_test")
    result += generate_golden_test("basic-with-inserts-deletes-checkpoint","latest_snapshot_test")
    result += generate_golden_test("basic-with-inserts-merge", "latest_snapshot_test")
    result += generate_golden_test("basic-with-inserts-overwrite-restore", "latest_snapshot_test")
    result += generate_golden_test("basic-with-inserts-updates", "latest_snapshot_test")
    result += generate_golden_test("basic-with-vacuum-protocol-check-feature","latest_snapshot_test")
    result += generate_golden_test("corrupted-last-checkpoint-kernel", "latest_snapshot_test")
    result += generate_golden_test("data-reader-array-complex-objects", "latest_snapshot_test")
    result += generate_golden_test("data-reader-array-primitives", "latest_snapshot_test")
    result += generate_golden_test("data-reader-date-types-America", "latest_snapshot_test")
    result += generate_golden_test("data-reader-date-types-Asia", "latest_snapshot_test")
    result += generate_golden_test("data-reader-date-types-Etc", "latest_snapshot_test")
    result += generate_golden_test("data-reader-date-types-Iceland", "latest_snapshot_test")
    result += generate_golden_test("data-reader-date-types-Jst", "latest_snapshot_test")
    result += generate_golden_test("data-reader-date-types-Pst", "latest_snapshot_test")
    result += generate_golden_test("data-reader-date-types-utc", "latest_snapshot_test")
    result += generate_golden_test("data-reader-escaped-chars", "latest_snapshot_test")
    result += generate_golden_test("data-reader-map", "latest_snapshot_test")
    result += generate_golden_test("data-reader-nested-struct", "latest_snapshot_test")
    result += generate_golden_test("data-reader-nullable-field-invalid-schema-key","latest_snapshot_test")
    result += generate_golden_test("data-reader-primitives", "latest_snapshot_test")
    result += generate_golden_test("data-reader-timestamp_ntz", "latest_snapshot_test")
    result += generate_golden_test("data-reader-timestamp_ntz-id-mode", "latest_snapshot_test")
    result += generate_golden_test("data-reader-timestamp_ntz-name-mode", "latest_snapshot_test")
    result += generate_golden_test("data-skipping-basic-stats-all-types", "latest_snapshot_test")
    result += generate_golden_test("data-skipping-basic-stats-all-types-checkpoint","latest_snapshot_test")
    result += generate_golden_test("data-skipping-basic-stats-all-types-columnmapping-name","latest_snapshot_test")
    result += generate_golden_test("data-skipping-change-stats-collected-across-versions",    "latest_snapshot_test")
    result += generate_golden_test("data-skipping-partition-and-data-column","latest_snapshot_test")
    result += generate_golden_test("decimal-various-scale-precision", "latest_snapshot_test")
    result += generate_golden_test("deltalog-getChanges", "latest_snapshot_test")
    result += generate_golden_test("dv-partitioned-with-checkpoint", "latest_snapshot_test")
    result += generate_golden_test("dv-with-columnmapping", "latest_snapshot_test")
    result += generate_golden_test("kernel-timestamp-int96", "latest_snapshot_test")
    result += generate_golden_test("kernel-timestamp-pst", "latest_snapshot_test")
    result += generate_golden_test("kernel-timestamp-timestamp_micros", "latest_snapshot_test")
    result += generate_golden_test("kernel-timestamp-timestamp_millis", "latest_snapshot_test")
    result += generate_golden_test("log-replay-dv-key-cases", "latest_snapshot_test")
    result += generate_golden_test("log-replay-latest-metadata-protocol", "latest_snapshot_test")
    result += generate_golden_test("log-replay-special-characters", "latest_snapshot_test")
    result += generate_golden_test("log-replay-special-characters-a", "latest_snapshot_test")
    result += generate_golden_test("multi-part-checkpoint", "latest_snapshot_test")
    result += generate_golden_test("only-checkpoint-files", "latest_snapshot_test")
    result += generate_golden_test("snapshot-data0", "latest_snapshot_test")
    result += generate_golden_test("snapshot-data1", "latest_snapshot_test")
    result += generate_golden_test("snapshot-data2", "latest_snapshot_test")
    result += generate_golden_test("snapshot-data2-deleted", "latest_snapshot_test")
    result += generate_golden_test("snapshot-data3", "latest_snapshot_test")
    result += generate_golden_test("snapshot-repartitioned", "latest_snapshot_test")
    result += generate_golden_test("snapshot-vacuumed", "latest_snapshot_test")
    result += generate_golden_test("time-travel-partition-changes-a", "latest_snapshot_test")
    result += generate_golden_test("time-travel-partition-changes-b", "latest_snapshot_test")
    result += generate_golden_test("time-travel-schema-changes-a", "latest_snapshot_test")
    result += generate_golden_test("time-travel-schema-changes-b", "latest_snapshot_test")
    result += generate_golden_test("time-travel-start", "latest_snapshot_test")
    result += generate_golden_test("time-travel-start-start20", "latest_snapshot_test")
    result += generate_golden_test("time-travel-start-start20-start40", "latest_snapshot_test")
    result += generate_golden_test("v2-checkpoint-json", "latest_snapshot_test")
    result += generate_golden_test("v2-checkpoint-parquet", "latest_snapshot_test")
    result += generate_golden_test("basic-decimal-table", "latest_snapshot_test")
    result += generate_golden_test("basic-decimal-table-legacy", "latest_snapshot_test")
    result += generate_golden_test("table-with-columnmapping-mode-name", "latest_snapshot_test")
    result += generate_golden_test("table-with-columnmapping-mode-id", "latest_snapshot_test")

    result += generate_negative_test("deltalog-invalid-protocol-version")
    result += generate_negative_test("deltalog-state-reconstruction-from-checkpoint-missing-metadata")
    result += generate_negative_test("deltalog-state-reconstruction-from-checkpoint-missing-protocol")
    result += generate_negative_test("deltalog-state-reconstruction-without-metadata")
    result += generate_negative_test("deltalog-state-reconstruction-without-protocol")
    result += generate_negative_test("no-delta-log-folder")
    result += generate_negative_test("versions-not-contiguous")

    # SKIPPED BY KERNEL TOO
    result += skip_test("data-skipping-basic-stats-all-types-columnmapping-id", "id column mapping mode not supported")
    result += skip_test("hive", "test not yet implemented - different file structure")
    result += skip_test("parquet-all-types", "schemas disagree about nullability, need to figure out which is correct and adjust")
    result += skip_test("parquet-all-types-legacy-format", "legacy parquet has name `array`, we should have adjusted this to `element`")
    result += skip_test("data-reader-partition-values", "Golden data needs to have 2021-09-08T11:11:11+00:00 as expected value for as_timestamp col")
    result += skip_test("canonicalized-paths-normal-a", "BUG: path canonicalization")
    result += skip_test("canonicalized-paths-normal-b", "BUG: path canonicalization")
    result += skip_test("delete-re-add-same-file-different-transactions", "test not yet implemented")
    result += skip_test("log-replay-special-characters-b", "test not yet implemented")

    # SKIPPED BECAUSE canonicalized_paths_test
    result += generate_golden_test("canonicalized-paths-special-a", "canonicalized_paths_test")
    result += generate_golden_test("canonicalized-paths-special-b", "canonicalized_paths_test")

    # SKIPPED BECAUSE checkpoint_test
    result += generate_golden_test("checkpoint", "checkpoint_test")

    with open(file, "w") as f:
        f.write(result)

def write_slow_tests():
    name = "generated"
    file_name = "generated_slow.test"
    file = f"test/sql/golden_tests/{name}.test_slow"
    result = generate_test_header(name, file_name)

    # These do relatively big result comparisons
    result += generate_golden_test("parquet-decimal-dictionaries", "latest_snapshot_test")
    result += generate_golden_test("parquet-decimal-dictionaries", "latest_snapshot_test")
    result += generate_golden_test("parquet-decimal-dictionaries-v1", "latest_snapshot_test")
    result += generate_golden_test("parquet-decimal-dictionaries-v2", "latest_snapshot_test")
    result += generate_golden_test("parquet-decimal-type", "latest_snapshot_test")

    with open(file, "w") as f:
        f.write(result)

write_base_tests()
write_slow_tests()