---
name: stdf-to-csv
description: Convert one or more STDF V4 binary test files into separate CSV files with per-test rows and lot, wafer, site, and part context. Use for batch STDF or STD exports, including gzip-compressed inputs; not for STDF V3 or legacy VAX floating-point files.
---

# STDF to CSV

Use the bundled converter to turn each input STDF file into its own CSV. The default output has one row per `PTR`, `MPR`, or `FTR` test result and repeats the relevant lot/wafer/site/part context on every row.

## Convert files

Run:

```bash
python3 scripts/stdf_to_csv.py <files-or-directories> -o <output-directory>
```

Useful options:

- Add `--recursive` when an input is a directory tree.
- Use `--workers N` to tune parallel file conversion; the default automatically uses up to four workers.
- Add `--overwrite` only when existing output files should be replaced.
- Use `--encoding utf-8` when a UTF-8 BOM is not wanted.

The command prints a JSON summary and exits with status `1` if any input fails. Read that summary, report each output path and row count, and surface per-file errors rather than retrying or overwriting automatically.

## Conversion contract

- Output one file per input, named from the input stem; collisions receive `__2`, `__3`, and so on.
- Accept `.stdf`, `.std`, `.stdf.gz`, and `.std.gz`. Compression is also detected by gzip magic bytes.
- Treat `result` and the limits as display-scaled values using `10 ** SCAL`; retain `result_raw` and raw limits alongside them.
- Leave numeric `result` blank when STDF flags mark the measurement unusable, while retaining the raw value for diagnosis.
- Assign interleaved parallel-site records by `HEAD_NUM + SITE_NUM`.
- An unfinished part sequence at a new `PIR` or at end-of-file is emitted with test context but without a fabricated part result.

The parser supports STDF V4 IEEE files written by `CPU_TYPE 1` or `2`. It counts but does not decode vendor-specific or non-test records. If a file uses STDF V3 or `CPU_TYPE 0` DEC VAX floating point, report the unsupported format instead of claiming a complete conversion.
