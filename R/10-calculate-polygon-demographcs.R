#######################
source("R/01-setup.R")
#######################

#The code provided aims to report various demographic statistics related to different population groups within the context of the isochrones created for accessibility analysis. It begins by reading in necessary data files, such as regions, state FIPS codes, and precomputed summary statistics for specific population groups within the isochrones. It then proceeds to generate a series of informative messages, each highlighting different aspects of the population covered by the isochrones. These messages include information about the total female population, the number of women from different racial and ethnic backgrounds, and the degree of coverage within the isochrones for each group. The code uses formatted and rounded numbers to present this information in a human-readable format. 

regions <- read.csv("data/fips-appalachia-delta.csv", colClasses = c("fips_county" = "character"))
state_fips <- read.csv("data/fips-states.csv", colClasses = "character")
state_fips <- state_fips %>% select(fips_state, state_code)

# Jesus I did this in exploratory and it just works
state_sums <- read_csv("data/08.5-prep-the-census-variables/end_state_sums.csv")

paste0("within_total:  ", format_and_round(state_sums$within_total), " is the total female population of all ages covered by the isochrones: sum(population * overlap)")

paste0("population_total: ", format_and_round(state_sums$population_total), " is the total female population.")

paste0("within_black: ", format_and_round(state_sums$within_black), " is the number of black women of all ages covered by the isochrones.")

paste0("population_black: ", format_and_round(state_sums$population_black), " is the total Black female population.")

paste0("within_white: ", format_and_round(state_sums$within_white), " is the number of white women of all ages covered by the isochrones.")

paste0("population_white: ", format_and_round(state_sums$population_white), " is the total White female population.")

paste0("within_two_numbers: ", format_and_round(state_sums$within_two_number), " is the number of women of two races of all ages covered by the isochrones.")

paste0("population_two_number: ", format_and_round(state_sums$population_two_number), " is the number of women describing two races of all ages covered by the isochrones.")

paste0("within_nhpi: ", format_and_round(state_sums$within_nhpi), " is the number of Native Hawaiian/Pacific Islander women population of all ages covered by the isochrones.")

paste0("population_nhpi: ", format_and_round(state_sums$population_nhpi), " is the number of women describing Native Hawaiian/Pacific Islanders of all ages.")

paste0("within_hispanic: ", format_and_round(state_sums$within_hispanic), " is the number of Hispanic women population of all ages covered by the isochrones.")

paste0("population_hispanic ", format_and_round(state_sums$population_hispanic), " is the number of women describing Hispanic women of all ages.")





# summary(bg$overlap)
# state_sums <- bg %>% #select(state_code, geoid, population, race_black_number, race_white_number) %>%
#   #pivot_longer(cols = starts_with("overlap_"), names_to = "certification_type", values_to = "overlap") %>%
#   group_by(state_code) %>%
#   summarize(within_total = sum(population * overlap),
#             population_total = sum(population),
#             within_black = sum(race_black_number * overlap),
#             population_black = sum(race_black_number),
#             within_white = sum(race_white_number * overlap),
#             population_white = sum(race_white_number)) %>%
#   mutate(within_total_pct = within_total/population_total,
#          within_black_pct = within_black/population_black,
#          within_white_pct = within_white/population_white) %>%
#   select(state_code, certification_type, ends_with("_pct"), everything()) %>%
#   ungroup() %>%
#   mutate(certification_type = case_when(
#     certification_type == "overlap_all" ~ "any",
#     certification_type == "overlap_c" ~ "compthromb"
#   ))


# Make table for use in story, charts
# Ridiculous pivoting but it works
# state_table <- state_sums %>% select(state_code, certification_type,
#                                      within_total_pct, within_black_pct, within_white_pct) %>%
#   pivot_wider(names_from = certification_type, values_from = starts_with("within"), names_prefix = "type_") %>%
#   pivot_longer(-state_code, names_sep = "_pct_type_",
#                names_to = c("race", "certification_type"), values_to = "within_pct") %>%
#   pivot_wider(names_from = certification_type, values_from = within_pct) %>%
#   rename(within_any = any, within_compthromb = compthromb) %>%
#   mutate(within_none = 1 - within_any,
#          within_primaryacute_only = within_any - within_compthromb) %>%
#   select(state_code, race, within_none, within_primaryacute_only, within_compthromb, within_any) %>%
#   mutate(race = case_when(race == "within_total" ~ "Total",
#                           race == "within_black" ~ "Black",
#                           race == "within_white" ~ "White"))
#
# write.csv(state_table, "data/state-stroke-access.csv", na = "", row.names = F)

# Chart data
# state_chart <- state_table %>% filter(race == "Total" & state_code != "NY") %>%
#   arrange(within_any)
# 
# # Add names
# state_names <- read.csv("data/fips-states.csv", colClasses = "character")
# state_names <- state_names %>% select(state_name, state_code)
# state_chart <- left_join(state_chart, state_names, by = "state_code")
# state_chart <- state_chart %>% select(state_name, within_none, within_primaryacute_only, within_compthromb)
# 
# write.csv(state_chart, "data/state-stroke-chart.csv", na = "", row.names = F)
#
# ###########################################################################
# # Calculate population in/out of isochrones by region
# ###########################################################################
# regions_min <- regions %>% select(fips_county, delta, appalachia)
# bg <- left_join(bg, regions_min, by = "fips_county")
#
# delta <- bg %>% filter(delta == 1 )%>%
#   select(geoid, overlap_all, overlap_c, population, race_black_number, race_white_number) %>%
#   pivot_longer(cols = starts_with("overlap_"), names_to = "certification_type", values_to = "overlap") %>%
#   group_by(certification_type) %>%
#   summarize(within_total = sum(population * overlap),
#             population_total = sum(population),
#             within_black = sum(race_black_number * overlap),
#             population_black = sum(race_black_number),
#             within_white = sum(race_white_number * overlap),
#             population_white = sum(race_white_number)) %>%
#   mutate(within_total_pct = within_total/population_total,
#          within_black_pct = within_black/population_black,
#          within_white_pct = within_white/population_white) %>%
#   mutate(region = "Mississippi Delta") %>%
#   select(region, certification_type, ends_with("_pct"), everything()) %>%
#   ungroup() %>%
#   mutate(certification_type = case_when(
#     certification_type == "overlap_all" ~ "any",
#     certification_type == "overlap_c" ~ "compthromb"
#   ))
#
# appalachia <- bg %>% filter(appalachia == 1 )%>%
#   select(geoid, overlap_all, overlap_c, population, race_black_number, race_white_number) %>%
#   pivot_longer(cols = starts_with("overlap_"), names_to = "certification_type", values_to = "overlap") %>%
#   group_by(certification_type) %>%
#   summarize(within_total = sum(population * overlap),
#             population_total = sum(population),
#             within_black = sum(race_black_number * overlap),
#             population_black = sum(race_black_number),
#             within_white = sum(race_white_number * overlap),
#             population_white = sum(race_white_number)) %>%
#   mutate(within_total_pct = within_total/population_total,
#          within_black_pct = within_black/population_black,
#          within_white_pct = within_white/population_white) %>%
#   mutate(region = "Appalachia") %>%
#   select(region, certification_type, ends_with("_pct"), everything()) %>%
#   ungroup() %>%
#   mutate(certification_type = case_when(
#     certification_type == "overlap_all" ~ "any",
#     certification_type == "overlap_c" ~ "compthromb"
#   ))
#
# region_sums <- bind_rows(appalachia, delta)
#
# # Make table for use in story, charts
# # Ridiculous pivoting but it works
# region_table <- region_sums %>% select(region, certification_type,
#                                        within_total_pct, within_black_pct, within_white_pct) %>%
#   pivot_wider(names_from = certification_type, values_from = starts_with("within"), names_prefix = "type_") %>%
#   pivot_longer(-region, names_sep = "_pct_type_",
#                names_to = c("race", "certification_type"), values_to = "within_pct") %>%
#   pivot_wider(names_from = certification_type, values_from = within_pct) %>%
#   rename(within_any = any, within_compthromb = compthromb) %>%
#   mutate(within_none = 1 - within_any,
#          within_primaryacute_only = within_any - within_compthromb) %>%
#   select(region, race, within_none, within_primaryacute_only, within_compthromb, within_any) %>%
#   mutate(race = case_when(race == "within_total" ~ "Total",
#                           race == "within_black" ~ "Black",
#                           race == "within_white" ~ "White"))
# write.csv(region_table, "data/appalachia-delta-stroke-access.csv", na = "", row.names = F)
