# Database Performance Suggestions

The following list summarizes 19 potential optimizations that can help speed up database operations in this project. Each bullet references a file and an approximate location for clarity.

1. **Add indexes when connecting to DuckDB** (e.g., in `R/bespoke_functions.R` lines 1-40) to accelerate lookups by NPI, taxonomy code, and entity type.
2. **Avoid loading entire tables into memory** in scripts like `R/C-Extracting_and_Processing_NPPES_Provider_Data.R` around line 1200; use `dbWriteTable` only for necessary subsets.
3. **Remove duplicate package loads** in `zzTo_Walker_for_consult/HERE_isochrone_results/01-setup.R` to shorten startup time.
4. **Use parameterized queries** rather than string interpolation in `R/zz2.33-toy example.R` lines 95-125 to prevent repeated parsing.
5. **Batch inserts with `dbAppendTable`** for large data writes instead of repeated `dbWriteTable` calls in scripts such as `zzC-Extracting_and_Processing_NPPES.R` around line 150.
6. **Create persistent connections** rather than reconnecting inside loops—see `R/bespoke_functions.R` lines 1200+ where new connections are opened repeatedly.
7. **Leverage `dbplyr` lazy tables** to filter directly in the database instead of collecting full tables first; see `R/bespoke_functions.R` inside `try_dplyr_approach`.
8. **Limit selected columns** in queries by explicitly listing required fields; example in `R/bespoke_functions.R` `try_sql_approach` lines 214-230.
9. **Ensure `dbDisconnect()` is called** at the end of each script to avoid orphan connections, e.g., `R/C-Extracting_and_Processing_NPPES_Provider_Data.R` line 1360.
10. **Use `PRAGMA` statements** to adjust DuckDB memory settings when handling large datasets (add near connection setup in `R/bespoke_functions.R`).
11. **Avoid `View()` calls** in scripts such as `R/C-Extracting_and_Processing_NPPES_Provider_Data.R` line 1242 which implicitly load entire tables into memory.
12. **Use transaction blocks** for multiple `dbExecute` calls to reduce disk I/O, e.g., within `zzUntitled3.R` around lines 1450+.
13. **Check for existing indexes before creation** to reduce overhead—implemented via `CREATE INDEX IF NOT EXISTS` as in the new function.
14. **Cache table-year mappings** instead of recalculating them each run (`create_nppes_table_mapping` results can be serialized to disk).
15. **Use `duckdb::duckdb_shutdown()`** after closing the connection to fully release resources (see `R/bespoke_functions.R` connection teardown).
16. **Vectorize tax code filtering** by preparing a single SQL `IN` clause rather than looping over codes; `try_sql_approach` demonstrates this but can be made clearer.
17. **Consider using `dbExecute` for bulk DELETE/UPDATE** instead of row-wise operations when cleaning tables, e.g., in data processing scripts.
18. **Avoid writing large intermediary CSV files** when they can be stored in DuckDB directly, as seen around line 168 in `zzC-Extracting_and_Processing_NPPES.R`.
19. **Profile query plans** with `EXPLAIN` in DuckDB to spot slow joins—add optional diagnostics in `R/bespoke_functions.R` before complex queries.

These suggestions target common bottlenecks in reading and writing large datasets. Implementing them can substantially speed up data preparation and analysis.
