# Changelog

## 2025-06-16
- Removed an extra blank line before the `# fin` comment in `R/01-setup.R` for cleaner formatting.

## 2025-06-22
- Hardened deduplication logic in `B-physician_compare_data_download.R` to avoid dynamic
  code execution.
- Sanitized parameters passed to `wget` in `A - nber_nppes_download.R` to prevent command
  injection.
- Restricted JavaScript `eval` usage in `GO_access_analysis_code.html`.
