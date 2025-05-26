# Load required packages
library(logger)
library(dplyr)
library(tidyr)
library(assertthat)
library(lubridate)
library(stringr)

# Configure logger
logger::log_threshold(logger::INFO)
logger::log_layout(logger::layout_glue_generator(
  format = "{level} [{time}] {msg}"
))

# Define the list of provider NPIs to check
provider_npis <- c(
  "1689603763",   # Tyler Muffly, MD
  "1528060639",   # John Curtin, MD
  "1346355807",   # Pedro Miranda, MD
  "1437904760",   # Lizeth Acosta, MD
  "1568738854"    # Aaron Lazorwitz, MD
)

# Define provider names (corresponding to the NPIs above)
provider_names <- c(
  "Tyler Muffly, MD",
  "John Curtin, MD",
  "Pedro Miranda, MD",
  "Lizeth Acosta, MD",
  "Aaron Lazorwitz, MD"
)

# Log start of quality check process
logger::log_info("Starting Medicare Part D Provider Quality Check")
logger::log_info("Checking presence for {length(provider_npis)} providers")

# Load Medicare data from file with proper column renaming
medicare_data_path <- "/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/final/end_Medicare_part_D_prescribers_combined_df.csv"
medicare_data <- readr::read_csv(medicare_data_path) %>%
  dplyr::rename(npi = PRSCRBR_NPI) %>%
  dplyr::rename(total_claim_count = Tot_Clms)

# Check that medicare_data exists and has expected columns
assertthat::assert_that(!is.null(medicare_data), 
                        msg = "Medicare data not loaded properly")
assertthat::assert_that(all(c("npi", "year") %in% colnames(medicare_data)),
                        msg = "Required columns 'npi' and 'year' not found in medicare_data")

# Log the dimensions of the loaded data
logger::log_info("Loaded Medicare data: {nrow(medicare_data)} rows, {ncol(medicare_data)} columns")
logger::log_info("Year range in data: {min(medicare_data$year)} to {max(medicare_data$year)}")

# Create name lookup for easier reference
name_lookup <- tibble::tibble(
  npi = provider_npis,
  provider_name = provider_names
)

# Create summary of provider presence
logger::log_info("Analyzing provider presence across years")
provider_summary <- medicare_data %>%
  dplyr::filter(npi %in% provider_npis) %>%
  dplyr::group_by(npi, year) %>%
  dplyr::summarize(
    record_count = dplyr::n(),
    total_claim_count = sum(total_claim_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  dplyr::arrange(npi, year)

# Log the record counts found
logger::log_info("Found {nrow(provider_summary)} year-provider combinations")

# Create comprehensive provider presence check
provider_presence <- provider_npis %>%
  tibble::tibble(npi = .) %>%
  dplyr::left_join(name_lookup, by = "npi") %>%
  dplyr::mutate(
    records_found = npi %in% provider_summary$npi
  )

# For providers found in the data, calculate detailed metrics
if (nrow(provider_summary) > 0) {
  # Get all available years in the dataset
  all_years <- sort(unique(medicare_data$year))
  logger::log_info("Dataset contains records for years: {paste(all_years, collapse = ', ')}")
  
  # Prepare summary statistics by provider
  provider_metrics <- provider_summary %>%
    dplyr::group_by(npi) %>%
    dplyr::summarize(
      total_records = sum(record_count),
      years_present = paste(sort(year), collapse = ", "),
      year_count = dplyr::n_distinct(year),
      earliest_year = min(year),
      latest_year = max(year),
      total_claims = sum(total_claim_count),
      .groups = "drop"
    ) 
  
  provider_metrics$npi <- as.numeric(provider_metrics$npi)
  provider_presence$npi <- as.numeric(provider_presence$npi)
  
  # Check for consecutive years
  provider_metrics <- provider_metrics %>%
    dplyr::rowwise() %>%
    dplyr::mutate(
      year_vector = list(as.numeric(strsplit(years_present, ", ")[[1]])),
      is_consecutive = all(diff(sort(unlist(year_vector))) == 1),
      years_missing = paste(setdiff(all_years, unlist(year_vector)), collapse = ", ")
    ) %>%
    dplyr::select(-year_vector)
  
  # Join metrics back to the presence check
  provider_presence <- provider_presence %>%
    dplyr::left_join(provider_metrics, by = "npi")
}

# Fill in missing values for providers not found
provider_presence <- provider_presence %>%
  dplyr::mutate(
    total_records = tidyr::replace_na(total_records, 0),
    years_present = ifelse(is.na(years_present), "None", years_present),
    year_count = tidyr::replace_na(year_count, 0),
    earliest_year = tidyr::replace_na(earliest_year, NA),
    latest_year = tidyr::replace_na(latest_year, NA),
    total_claims = tidyr::replace_na(total_claims, 0),
    is_consecutive = tidyr::replace_na(is_consecutive, FALSE),
    years_missing = ifelse(is.na(years_missing), paste(all_years, collapse = ", "), years_missing)
  )

# Log summary of findings
logger::log_info("Quality check complete. Summary of findings:")
logger::log_info("Providers found in dataset: {sum(provider_presence$records_found)}/{nrow(provider_presence)}")

# Print detailed findings for each provider
for (i in 1:nrow(provider_presence)) {
  if (provider_presence$records_found[i]) {
    logger::log_info("Provider {provider_presence$provider_name[i]} ({provider_presence$npi[i]}):")
    logger::log_info("  - Total records: {provider_presence$total_records[i]}")
    logger::log_info("  - Years present: {provider_presence$years_present[i]}")
    logger::log_info("  - Consecutive years: {ifelse(provider_presence$is_consecutive[i], 'Yes', 'No')}")
    logger::log_info("  - Total claims: {provider_presence$total_claims[i]}")
  } else {
    logger::log_warn("Provider {provider_presence$provider_name[i]} ({provider_presence$npi[i]}) not found in dataset")
  }
}

# Calculate overall data quality metrics
if (nrow(provider_summary) > 0) {
  coverage_percent <- mean(provider_presence$records_found) * 100
  years_coverage <- provider_presence %>%
    dplyr::filter(records_found) %>%
    dplyr::summarize(
      avg_years = mean(year_count),
      median_years = median(year_count),
      max_years = max(year_count),
      consecutive_percent = mean(is_consecutive) * 100
    )
  
  logger::log_info("Overall quality metrics:")
  logger::log_info("  - Provider coverage: {round(coverage_percent, 1)}%")
  logger::log_info("  - Average years per provider: {round(years_coverage$avg_years, 1)}")
  logger::log_info("  - Providers with consecutive years: {round(years_coverage$consecutive_percent, 1)}%")
}

# Check for data anomalies
if (nrow(provider_summary) > 0) {
  # Check for unusually high claim counts (potential data issues)
  high_claims <- provider_summary %>%
    dplyr::filter(total_claim_count > 10000) %>%
    dplyr::arrange(desc(total_claim_count))
  
  if (nrow(high_claims) > 0) {
    logger::log_warn("Found {nrow(high_claims)} instances of unusually high claim counts")
    logger::log_warn("Top anomalies:")
    for (i in 1:min(3, nrow(high_claims))) {
      provider_name <- name_lookup$provider_name[name_lookup$npi == high_claims$npi[i]]
      logger::log_warn("  - {provider_name} ({high_claims$npi[i]}): {high_claims$total_claim_count[i]} claims in {high_claims$year[i]}")
    }
  } else {
    logger::log_info("No unusually high claim counts detected")
  }
  
  # Check for gaps in yearly data
  providers_with_gaps <- provider_presence %>%
    dplyr::filter(records_found & !is_consecutive & year_count > 1)
  
  if (nrow(providers_with_gaps) > 0) {
    logger::log_warn("Found {nrow(providers_with_gaps)} providers with non-consecutive years")
    for (i in 1:nrow(providers_with_gaps)) {
      logger::log_warn("  - {providers_with_gaps$provider_name[i]}: Present in {providers_with_gaps$years_present[i]}")
    }
  } else {
    logger::log_info("No providers with gaps in yearly data detected")
  }
}

# Check for missing 'Prescribed' information
prescribed_missing <- medicare_data %>%
  dplyr::filter(npi %in% provider_npis & is.na(Prescribed)) %>%
  dplyr::count(npi) %>%
  dplyr::arrange(desc(n))

if (nrow(prescribed_missing) > 0) {
  logger::log_warn("Found {nrow(prescribed_missing)} providers with missing prescription information")
  for (i in 1:nrow(prescribed_missing)) {
    provider_name <- name_lookup$provider_name[name_lookup$npi == prescribed_missing$npi[i]]
    logger::log_warn("  - {provider_name} ({prescribed_missing$npi[i]}): {prescribed_missing$n[i]} records with missing 'Prescribed' information")
  }
} else {
  logger::log_info("No missing prescription information detected for checked providers")
}

# Save quality check results
quality_check_output_path <- "/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/analysis/provider_quality_check_results.csv"
readr::write_csv(provider_presence, quality_check_output_path)
logger::log_info("Quality check results saved to: {quality_check_output_path}")

logger::log_info("Medicare Part D Provider Quality Check completed")

####
# QI Plots
####

# Load required packages for visualization
library(ggplot2)
library(dplyr)
library(tidyr)
library(patchwork)
library(lubridate)
library(scales)

# Assuming provider_presence and provider_summary datasets are already created
# from the previous quality check code

# 1. Create a plot showing provider presence across years
logger::log_info("Creating visualization of provider presence across years")

provider_summary$npi <- as.numeric(provider_summary$npi)
name_lookup$npi <- as.numeric(name_lookup$npi)

# Prepare data for year presence plot
years_presence_data <- provider_summary %>%
  dplyr::select(npi, year, total_claim_count) %>%
  dplyr::left_join(name_lookup, by = "npi")

# Create plot of presence by year
presence_plot <- ggplot(years_presence_data, aes(x = year, y = provider_name, size = total_claim_count)) +
  geom_point(alpha = 0.7, color = "darkblue") +
  scale_size_continuous(name = "Total Claims", labels = scales::comma) +
  labs(
    title = "Medicare Part D Provider Presence by Year",
    subtitle = "Circle size represents total claim count",
    x = "Year",
    y = "Provider Name"
  ) +
  theme_minimal() +
  theme(
    plot.title = element_text(face = "bold"),
    axis.text.y = element_text(size = 10),
    legend.position = "right"
  ); presence_plot

print("Lazorwitz has no prescriptions after 2018 because he does family planning now.")

# 2. Create a bar chart of total claims by provider
# 3. Create heatmap of prescriber activity by year
# First, reshape the data to create a matrix-like structure
if (nrow(provider_summary) > 0) {
  # Ensure provider_name is properly joined and preserved
  heatmap_data <- provider_summary %>%
    dplyr::select(npi, year, total_claim_count) %>%
    dplyr::left_join(name_lookup, by = "npi") %>%
    # Ensure all combinations exist
    tidyr::complete(
      npi = unique(provider_summary$npi),
      year = min(provider_summary$year):max(provider_summary$year),
      fill = list(total_claim_count = 0)
    ) %>%
    # Make sure the join doesn't drop provider_name
    dplyr::group_by(npi, year) %>%
    dplyr::mutate(
      provider_name = first(provider_name)
    ) %>%
    dplyr::ungroup()
  
  # Verify provider_name exists
  if (!"provider_name" %in% colnames(heatmap_data)) {
    # If still missing, join it again
    heatmap_data <- heatmap_data %>%
      dplyr::left_join(name_lookup, by = "npi")
  }
  
  # Debug: Check the structure
  print(str(heatmap_data))
  print(head(heatmap_data))
  
  # Create heatmap
  activity_heatmap <- ggplot(heatmap_data, aes(x = year, y = provider_name, fill = total_claim_count)) +
    geom_tile(color = "white") +
    scale_fill_gradient2(
      low = "white", 
      high = "darkred",
      mid = "orange",
      midpoint = max(heatmap_data$total_claim_count) / 2,
      name = "Total Claims",
      labels = scales::comma
    ) +
    labs(
      title = "Medicare Part D Prescribing Activity Heatmap",
      x = "Year",
      y = NULL
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(face = "bold"),
      axis.text.y = element_text(size = 10),
      legend.position = "right"
    )
}; activity_heatmap

# 4. Create a plot showing year coverage and gaps
year_coverage_plot <- NULL
if (exists("provider_metrics")) {
  # Create data frame for the year span visualization
  year_spans <- provider_metrics %>%
    dplyr::filter(year_count > 0) %>%
    dplyr::select(npi, earliest_year, latest_year) %>%
    dplyr::left_join(name_lookup, by = "npi") %>%
    dplyr::arrange(earliest_year)
  
  # Create plot
  year_coverage_plot <- ggplot(year_spans, aes(y = reorder(provider_name, earliest_year))) +
    geom_segment(
      aes(
        x = earliest_year, 
        xend = latest_year,
        yend = provider_name
      ),
      linewidth = 6, 
      color = "steelblue", 
      alpha = 0.7
    ) +
    geom_point(aes(x = earliest_year), size = 3, color = "darkblue") +
    geom_point(aes(x = latest_year), size = 3, color = "darkred") +
    labs(
      title = "Year Coverage by Provider",
      subtitle = "Blue dots: First year | Red dots: Last year",
      x = "Year",
      y = NULL
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(face = "bold"),
      axis.text.y = element_text(size = 10)
    )
}; year_coverage_plot

# 5. Create consistency analysis plot (consecutive vs non-consecutive)
consistency_plot <- NULL
if (exists("provider_presence") && "is_consecutive" %in% colnames(provider_presence)) {
  consistency_data <- provider_presence %>%
    dplyr::filter(records_found) %>%
    dplyr::mutate(
      consistency_status = ifelse(is_consecutive, "Consecutive Years", "Non-Consecutive Years")
    ) %>%
    dplyr::count(consistency_status) %>%
    dplyr::mutate(
      percentage = n / sum(n) * 100,
      label = paste0(round(percentage, 1), "%\n(", n, ")")
    )
  
  consistency_plot <- ggplot(consistency_data, aes(x = "", y = percentage, fill = consistency_status)) +
    geom_bar(stat = "identity", width = 1) +
    geom_text(aes(label = label), position = position_stack(vjust = 0.5), color = "white", size = 4) +
    coord_polar("y", start = 0) +
    scale_fill_manual(values = c("Consecutive Years" = "darkgreen", "Non-Consecutive Years" = "darkred")) +
    labs(
      title = "Provider Data Consistency Analysis",
      subtitle = "Proportion of providers with consecutive vs. non-consecutive years",
      fill = "Consistency Status"
    ) +
    theme_void() +
    theme(
      plot.title = element_text(face = "bold", hjust = 0.5),
      plot.subtitle = element_text(hjust = 0.5),
      legend.position = "bottom"
    )
}; consistency_plot

# Save plots to files
output_dir <- "/Volumes/Video Projects Muffly 1/MedicarePartDPrescribersbyProvider/analysis/plots"
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

logger::log_info("Saving plots to directory: {output_dir}")

# Save individual plots
ggsave(file.path(output_dir, "provider_presence_by_year.png"), presence_plot, width = 10, height = 6)
ggsave(file.path(output_dir, "total_claims_by_provider.png"), claims_plot, width = 10, height = 6)

if (exists("activity_heatmap")) {
  ggsave(file.path(output_dir, "prescribing_activity_heatmap.png"), activity_heatmap, width = 10, height = 6)
}

if (!is.null(year_coverage_plot)) {
  ggsave(file.path(output_dir, "year_coverage_by_provider.png"), year_coverage_plot, width = 10, height = 6)
}

if (!is.null(consistency_plot)) {
  ggsave(file.path(output_dir, "provider_consistency_analysis.png"), consistency_plot, width = 8, height = 8)
}
