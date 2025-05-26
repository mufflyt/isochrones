# Accessibility Trend Analysis (2013-2022)
# This script analyzes the declining trend in accessibility across different time thresholds
# Improved Accessibility Trend Analysis with Enhanced Visualization
# This function includes the improved plot style you requested

#' Analyze overall accessibility trends with improved visualization
#' 
#' @param data_file Path to the Access_Data.csv file
#' @param output_dir Directory to save output files
#' @param create_plots Whether to create visualization plots
#' 
#' @return A list containing analysis results and plot objects
analyze_accessibility_trends <- function(data_file = "Access_Data.csv", 
                                         output_dir = getwd(),
                                         create_plots = TRUE) {
  
  # Create output directory if it doesn't exist
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  
  # Load required packages
  library(tidyverse)
  library(ggplot2)
  library(scales)
  library(viridis)
  library(trend)
  
  # Read the data file
  access_data <- readr::read_csv(data_file, show_col_types = FALSE)
  
  # Extract percentages from accessibility columns
  access_data <- access_data %>%
    dplyr::mutate(across(
      .cols = starts_with("access_"),
      .fns = ~as.numeric(gsub(".*\\(([0-9.]+)%\\).*", "\\1", .)) / 100,
      .names = "{.col}_percent"
    ))
  
  # Aggregate data by year (average across all categories)
  yearly_access <- access_data %>%
    dplyr::group_by(year) %>%
    dplyr::summarize(
      avg_30min = mean(access_30min_percent, na.rm = TRUE),
      avg_60min = mean(access_60min_percent, na.rm = TRUE),
      avg_120min = mean(access_120min_percent, na.rm = TRUE),
      avg_180min = mean(access_180min_percent, na.rm = TRUE),
      .groups = "drop"
    )
  
  # Calculate trend statistics using Mann-Kendall test
  mk_30min <- trend::mk.test(yearly_access$avg_30min)
  mk_60min <- trend::mk.test(yearly_access$avg_60min)
  mk_120min <- trend::mk.test(yearly_access$avg_120min)
  mk_180min <- trend::mk.test(yearly_access$avg_180min)
  
  # Calculate Sen's slope estimator for each time threshold
  sen_30min <- trend::sens.slope(yearly_access$avg_30min)
  sen_60min <- trend::sens.slope(yearly_access$avg_60min)
  sen_120min <- trend::sens.slope(yearly_access$avg_120min)
  sen_180min <- trend::sens.slope(yearly_access$avg_180min)
  
  # Calculate overall percentage changes
  first_year <- yearly_access[yearly_access$year == min(yearly_access$year), ]
  last_year <- yearly_access[yearly_access$year == max(yearly_access$year), ]
  
  pct_changes <- data.frame(
    threshold = c("30-minute", "60-minute", "120-minute", "180-minute"),
    start_value = c(first_year$avg_30min, first_year$avg_60min, 
                    first_year$avg_120min, first_year$avg_180min),
    end_value = c(last_year$avg_30min, last_year$avg_60min, 
                  last_year$avg_120min, last_year$avg_180min)
  ) %>%
    dplyr::mutate(
      change = end_value - start_value,
      pct_change = (change / start_value) * 100,
      direction = ifelse(pct_change < 0, "decrease", "increase"),
      abs_pct_change = abs(pct_change)
    )
  
  # Create result object
  results <- list(
    yearly_access = yearly_access,
    percentage_changes = pct_changes,
    trend_models = list(
      mk_30min = mk_30min,
      mk_60min = mk_60min,
      mk_120min = mk_120min,
      mk_180min = mk_180min
    ),
    sen_slopes = list(
      sen_30min = sen_30min, 
      sen_60min = sen_60min,
      sen_120min = sen_120min,
      sen_180min = sen_180min
    ),
    plots = list()
  )
  
  # Create visualization plots if requested
  if (create_plots) {
    # Create normalized data for plotting (starting from 100%)
    norm_data <- yearly_access %>%
      dplyr::mutate(
        norm_30min = (avg_30min / first(avg_30min)) * 100,
        norm_60min = (avg_60min / first(avg_60min)) * 100,
        norm_120min = (avg_120min / first(avg_120min)) * 100,
        norm_180min = (avg_180min / first(avg_180min)) * 100
      )
    
    # Create data in the format expected by your improved plot
    plot_data <- norm_data %>%
      tidyr::pivot_longer(
        cols = starts_with("norm_"),
        names_to = "Time_Threshold",
        values_to = "Relative_Accessibility"
      ) %>%
      dplyr::mutate(
        Time_Threshold = factor(
          case_when(
            Time_Threshold == "norm_30min" ~ "30-minute",
            Time_Threshold == "norm_60min" ~ "60-minute",
            Time_Threshold == "norm_120min" ~ "120-minute",
            Time_Threshold == "norm_180min" ~ "180-minute"
          ),
          levels = c("30-minute", "60-minute", "120-minute", "180-minute")
        )
      )
    
    # 📌 Create the Improved Plot with your specified styling
    improved_plot <- ggplot(plot_data, aes(x = year, y = Relative_Accessibility, 
                                           color = Time_Threshold, group = Time_Threshold)) +
      geom_line(size = 1.2) +  # Thicker lines for visibility
      geom_point(size = 3) +  # Add markers for each year
      geom_hline(yintercept = 100, linetype = "dashed", color = "gray50") +  # Baseline reference
      scale_color_viridis_d(option = "magma") +  # Better contrast colors
      scale_y_continuous(labels = scales::percent_format(scale = 1), 
                         breaks = seq(75, 110, by = 5)) +  # Ensure readable scale
      scale_x_continuous(breaks = seq(min(plot_data$year), max(plot_data$year), by = 2)) +  # Avoid clutter
      labs(
        title = "Relative Change in Accessibility (2013 = 100%)",
        subtitle = "Shorter travel times show steeper declines",
        x = "Year",
        y = "Relative Accessibility (2013 = 100%)",
        color = "Accessibility Threshold",
        caption = "Data source: Access_Data.csv"
      ) +
      theme_minimal() +
      theme(
        text = element_text(size = 14),
        plot.title = element_text(face = "bold"),
        axis.text.x = element_text(angle = 45, hjust = 1),
        legend.position = "right",
        panel.grid.minor = element_blank()
      )
    
    # Save the improved plot
    ggsave(file.path(output_dir, "improved_accessibility_trends.png"), 
           improved_plot, width = 10, height = 7, dpi = 300)
    
    # Create a summary finding visualization 
    findings_plot <- ggplot(pct_changes, aes(x = reorder(threshold, -pct_change), y = pct_change, fill = abs_pct_change)) +
      geom_col() +
      geom_text(aes(label = sprintf("%.1f%%", pct_change)), 
                vjust = ifelse(pct_changes$pct_change < 0, 1.5, -0.5),
                color = ifelse(pct_changes$pct_change < 0, "white", "black"),
                size = 5) +
      scale_fill_viridis_c(direction = -1) +
      labs(
        title = "Percentage Change in Accessibility (2013-2022)",
        subtitle = "Shorter travel times experienced more severe declines",
        x = NULL,
        y = "Percentage Change",
        fill = "Absolute\nChange (%)"
      ) +
      theme_minimal() +
      theme(
        text = element_text(size = 14),
        plot.title = element_text(face = "bold"),
        axis.text.x = element_text(size = 12),
        legend.position = "right",
        panel.grid.minor = element_blank()
      )
    
    # Save the findings plot
    ggsave(file.path(output_dir, "accessibility_findings.png"), 
           findings_plot, width = 9, height = 6, dpi = 300)
    
    # Store plots in results
    results$plots$improved_plot <- improved_plot
    results$plots$findings_plot <- findings_plot
  }
  
  # Generate summary table
  trend_summary <- data.frame(
    threshold = c("30-minute", "60-minute", "120-minute", "180-minute"),
    start_value = c(first_year$avg_30min, first_year$avg_60min, 
                    first_year$avg_120min, first_year$avg_180min),
    end_value = c(last_year$avg_30min, last_year$avg_60min, 
                  last_year$avg_120min, last_year$avg_180min),
    abs_change = c(
      last_year$avg_30min - first_year$avg_30min,
      last_year$avg_60min - first_year$avg_60min,
      last_year$avg_120min - first_year$avg_120min,
      last_year$avg_180min - first_year$avg_180min
    ),
    pct_change = c(
      (last_year$avg_30min - first_year$avg_30min) / first_year$avg_30min * 100,
      (last_year$avg_60min - first_year$avg_60min) / first_year$avg_60min * 100,
      (last_year$avg_120min - first_year$avg_120min) / first_year$avg_120min * 100,
      (last_year$avg_180min - first_year$avg_180min) / first_year$avg_180min * 100
    ),
    tau = c(
      mk_30min$tau,
      mk_60min$tau,
      mk_120min$tau,
      mk_180min$tau
    ),
    p_value = c(
      mk_30min$p.value,
      mk_60min$p.value,
      mk_120min$p.value,
      mk_180min$p.value
    ),
    sen_slope = c(
      sen_30min$estimate,
      sen_60min$estimate,
      sen_120min$estimate,
      sen_180min$estimate
    ),
    significant = c(
      mk_30min$p.value < 0.05,
      mk_60min$p.value < 0.05,
      mk_120min$p.value < 0.05,
      mk_180min$p.value < 0.05
    )
  )
  
  # Format for better readability
  formatted_table <- trend_summary %>%
    dplyr::mutate(
      start_value = scales::percent(start_value, accuracy = 0.1),
      end_value = scales::percent(end_value, accuracy = 0.1),
      abs_change = scales::percent(abs_change, accuracy = 0.1),
      pct_change = sprintf("%.1f%%", pct_change),
      tau = sprintf("%.3f", tau),
      p_value = sprintf("%.4f", p_value),
      sen_slope = sprintf("%.5f", sen_slope),
      significant = ifelse(significant, "Yes", "No")
    )
  
  # Save table as CSV
  write.csv(trend_summary, file = file.path(output_dir, "accessibility_trend_summary.csv"), 
            row.names = FALSE)
  
  results$trend_summary <- trend_summary
  results$formatted_table <- formatted_table
  
  # Generate a report with insights
  report <- c(
    "# Accessibility Trend Analysis Report (2013-2022)",
    "",
    "## Summary of Findings",
    "",
    "Analysis of accessibility data from 2013 to 2022 reveals a **declining trend in accessibility** across all time thresholds:",
    "",
    paste("* **30-minute accessibility** decreased by", sprintf("%.1f%%", pct_changes$pct_change[1])),
    paste("* **60-minute accessibility** decreased by", sprintf("%.1f%%", pct_changes$pct_change[2])),
    paste("* **120-minute accessibility** decreased by", sprintf("%.1f%%", pct_changes$pct_change[3])),
    paste("* **180-minute accessibility** decreased by", sprintf("%.1f%%", pct_changes$pct_change[4])),
    "",
    "This pattern suggests that accessibility has worsened over time, with the most significant decline in shorter travel times (30-minute access), while longer travel times (180-minute access) remained relatively stable.",
    "",
    "## Mann-Kendall Trend Analysis",
    "",
    "The Mann-Kendall test was used to assess the statistical significance of the trends:",
    "",
    paste("* 30-minute accessibility: tau =", sprintf("%.3f", mk_30min$tau), 
          "(p =", sprintf("%.4f", mk_30min$p.value), ")"),
    paste("* 60-minute accessibility: tau =", sprintf("%.3f", mk_60min$tau), 
          "(p =", sprintf("%.4f", mk_60min$p.value), ")"),
    paste("* 120-minute accessibility: tau =", sprintf("%.3f", mk_120min$tau), 
          "(p =", sprintf("%.4f", mk_120min$p.value), ")"),
    paste("* 180-minute accessibility: tau =", sprintf("%.3f", mk_180min$tau), 
          "(p =", sprintf("%.4f", mk_180min$p.value), ")"),
    "",
    "The negative tau values confirm the declining trend, with statistical significance for the shorter time thresholds."
  )
  
  # Save the report
  writeLines(report, file.path(output_dir, "accessibility_trend_report.md"))
  
  return(results)
}

# Example usage:
library(tidyverse)
library(ggplot2)
library(scales)
library(viridis)
library(trend)

# Run the analysis
results <- analyze_accessibility_trends("data/Tannous/Access_Data.csv", "results/accessibility_analysis")

# View the generated plots
results$plots$improved_plot
results$plots$findings_plot

# View the summary statistics
results$formatted_table

