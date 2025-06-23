# File Save Messages

All functions that write files now print a message indicating where the file was
saved. This makes it easier to verify outputs when running long workflows.

Use `message()` or `logger::log_info()` directly after the write call, for
example:

```r
readr::write_csv(data, "output.csv")
message("Saved data to output.csv")
```
