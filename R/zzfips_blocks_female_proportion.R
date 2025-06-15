
# required R packages
library(tigris)
library(censusapi)
library(dplyr)
library(magrittr)
library(doParallel)
library(foreach)
library(parallel)

# set the working directory (optional, default is current directory)

# Store tidycensus data on cache
options(tigris_use_cache = TRUE)
options(tigris_class = "sf")
getOption("tigris_use_cache")

# number of threads to use in the R script
threads = parallel::detectCores()

# get the US fips
us_fips_list <- tigris::fips_codes %>%
  dplyr::select(state_code, state_name) %>%
  dplyr::distinct(state_code, .keep_all = TRUE) %>%
  # dplyr::filter(state_code < 56) %>%                 # I commented out this line because the minutes isochrones might be included in all state fips
  #dplyr::filter(state_code < 56 | state_code == "72") %>%
  dplyr::select(state_code) %>%
  dplyr::pull()


# directory to save the results for each fips & minute-isochrones
dir_save = file.path(getwd(), 'data', 'save_fips_isochrones')
if (!dir.exists(dir_save)) dir.create(dir_save, recursive = TRUE)

# directory of the .shp files
dir_csv = file.path(getwd(),'data', '08-get-block-group-overlap')
if (!dir.exists(dir_csv)) stop(glue::glue("The directory '{dir_csv}' does not exist!"))

# list the updated .csv files of the "data/08-get-block-group-overlap/" directory which start with "intersect_block_"
lst_csv = list.files(path = dir_csv, pattern = ".csv$", full.names = FALSE)
if (length(lst_csv) == 0) stop("The directory does not include any .csv files!")

#................................................................................................ we need the .csv to get the unique fips
# we keep the files starting with "intersect_block_*"
idx_csv = which(stringr::str_detect(string = lst_csv, pattern = '^intersect_block_'))
if (length(idx_csv) == 0) stop("We expect that the files start with 'intersect_block_*'!")

# keep the subset of the .csv files
lst_csv_subs = lst_csv[idx_csv]

# first of all we have to retrieve the fips of all computed isochrones
dat_all_isochr_csv = lapply(seq_along(lst_csv_subs), function(x) {
  file_x = lst_csv_subs[x]
  dat_x = data.table::fread(file = file.path(dir_csv, file_x), header = T, stringsAsFactors = F, nThread = threads)
  dat_x
})

# include also the names of the .csv files to the sublists
names(dat_all_isochr_csv) = gsub(pattern = '.csv', replacement = '', x = lst_csv_subs)

# get the fips of the computed isochrones, for which we have to download the tigris block geometries
fips_isochr_mins = lapply(dat_all_isochr_csv, function(x) {
  x_fips = sort(unique(x$STATEFP))
  x_fips = ifelse(nchar(x_fips) == 1, as.character(glue::glue("0{x_fips}")), as.character(x_fips))
  x_fips
}) |>
  unlist() |>
  unique()

#............................................................................................ we read also the isochrones

# directory of the .shp files
dir_shp = file.path(getwd(),'data', '08-get-block-group-overlap', 'isochrone_files')
if (!dir.exists(dir_shp)) stop(glue::glue("The directory '{dir_shp}' does not exist!"))

# list the updated .shp files of the "data/08-get-block-group-overlap/isochrone_files/" directory
lst_shp = list.files(path = dir_shp, pattern = ".shp$", full.names = FALSE)
if (length(lst_shp) == 0) stop("The directory does not include any .shp files!")

# we expect that the files start with "filtered_isochrones_*"
idx_shp = which(stringr::str_detect(string = lst_shp, pattern = '^filtered_isochrones'))
if (length(idx_shp) == 0) stop("We expect that the files start with 'filtered_isochrones_*'!")

# we keep only the relevant .shp files
lst_shp = lst_shp[idx_shp]

dat_all_isochr = lapply(seq_along(lst_shp), function(x) {
  file_x = lst_shp[x]
  dat_x = sf::st_read(file.path(dir_shp, file_x), quiet = TRUE)
  cat(glue::glue("Missing values in the 'pgender' column of the {unique(dat_x$ischrn__)} minutes isochrones:  {sum(is.na(dat_x$pgender))}"), '\n')
  dat_x
})

# include also the names of the .csv files to the sublists
names(dat_all_isochr) = gsub(pattern = '.shp', replacement = '', x = lst_shp)

#............................................................................................

# Function to GET THE ACS CENSUS VARIABLES
get_acs_data <- function(us_fips_list, vintage = 2019, acs_variables) {
  state_data <- list()
  for (fips_code in us_fips_list) {
    stateget <- paste("state:", fips_code, "&in=county:*&in=tract:*", sep = "")
    state_data[[fips_code]] <- censusapi::getCensus(name = "acs/acs5", vintage = vintage,
                                                    vars = c("NAME", acs_variables),
                                                    region = "block group:*", regionin = stateget,
                                                    key = "485c6da8987af0b9829c25f899f2393b4bb1a4fb")
  }
  
  acs_raw <- dplyr::bind_rows(state_data)
  acs_raw <- acs_raw %>%
    mutate(year = vintage)
  Sys.sleep(1)
  
  readr::write_csv(acs_raw, paste0("data/08.75-acs/08.75-acs_", vintage, "vintage.csv"))
  return(acs_raw)
}


# we now iterate over each one of the fips and we retrieve the following:
# - the acs census data
# - the tigris block geometries

# we specify the year
YEAR = 2021

for (FIPS_item in fips_isochr_mins) {
  
  cat(paste0(FIPS_item, '.'))
  
  DIR_FIPS = file.path(dir_save, FIPS_item)
  if (!dir.exists(DIR_FIPS)) dir.create(DIR_FIPS)
  
  #............................................................................................................................................ ACS data
  acs_variables <- c("B01001_001E", "B02001_001E", "B02001_002E", "B02001_003E", "B02001_004E", "B02001_005E", "B02001_006E")  
  acs_data <- get_acs_data(us_fips_list = FIPS_item,
                           vintage = YEAR,
                           acs_variables = acs_variables)
  
  demographics_bg <- acs_data %>%
    rename(population = B01001_001E, race_universe_number = B02001_001E, race_white_number = B02001_002E, race_black_number = B02001_003E, race_aian_number = B02001_004E, race_asian_number = B02001_005E, race_nhpi_number = B02001_006E) %>%
    mutate(race_other_number = race_universe_number - race_white_number - race_black_number - race_aian_number - race_asian_number - race_nhpi_number) %>%
    mutate(race_white_pct = race_white_number/race_universe_number) %>%
    mutate(race_black_pct = race_black_number/race_universe_number) %>%
    mutate(race_aian_pct = race_aian_number/race_universe_number) %>%
    mutate(race_asian_pct = race_asian_number/race_universe_number) %>%
    mutate(race_nhpi_pct = race_nhpi_number/race_universe_number) %>%
    mutate(race_other_pct = race_other_number/race_universe_number) %>%
    mutate(across(c(race_white_pct, race_black_pct, race_aian_pct, race_asian_pct, race_nhpi_pct, race_other_pct), ~ (round(., digits = 3)))) %>%
    rename(fips_state = state) %>%
    arrange(fips_state) %>%
    mutate(fips_county = paste0(fips_state, county)) %>%
    mutate(fips_tract = paste0(fips_state, county, tract)) %>%
    mutate(fips_block_group = paste0(fips_state, county, tract, block_group)) %>%
    rename(name = NAME) %>%
    select(fips_block_group, fips_state, fips_county, fips_tract, name, everything()) %>%
    select(-starts_with("B"), -contains("universe"), -county, -tract, -block_group)
  
  demographics_bg_item <- demographics_bg %>% 
    arrange(fips_block_group) %>% 
    data.table::as.data.table()
  
  #............................................................................................................................................ tigris block geometries
  
  # Download block shapefiles for a specific state and county
  blocks <- tigris::blocks(state = FIPS_item, year = YEAR)
  # dim(blocks)
  
  # first  digit of the "NAME20" column which is assumed to be the blogs 1-digit number
  blocks_1_digit = substr(x = blocks$BLOCKCE20, start = 1, stop = 1)
  
  blocks_id = glue::glue("{blocks$STATEFP20}{blocks$COUNTYFP20}{blocks$TRACTCE20}{blocks_1_digit}") |>
    as.character()
  
  tracks_id = glue::glue("{blocks$STATEFP20}{blocks$COUNTYFP20}{blocks$TRACTCE20}") |>
    as.character()
  
  # verify that we have the same number of digits between the 2 datasets
  if (all(nchar(blocks_id) == 12) != all(nchar(demographics_bg_item$fips_block_group) == 12)) stop("We expect that the fips_block_group ID has 12 characters in both datasets!")
  
  #............................................................................................................................................ subsets of tigris & demographics and merging
  
  blocks$fips_block_group = blocks_id
  blocks$fips_tracks_id = tracks_id
  
  blocks_subs = blocks[, c('fips_block_group', 'fips_tracks_id')] |>
    data.table::as.data.table()

  blocks_subs_spl = split(blocks_subs, by = 'fips_tracks_id')
  # length(blocks_subs_spl)
  # as.vector(unlist(lapply(blocks_subs_spl, nrow)))
  
  # subset of demographics
  demographics_subs = demographics_bg_item[, c('fips_block_group', 'population')]
  
  
  # parallelization of the for-loop
  # OS specific code snippets
  if (.Platform$OS.type == "unix") {
    doParallel::registerDoParallel(cores = threads)
  }
  if (.Platform$OS.type == "windows") {
    cl = parallel::makePSOCKcluster(threads)
    doParallel::registerDoParallel(cl = cl)
  }
  
  LEN = length(blocks_subs_spl)
  
  # release ram before parallelization
  for (k in 1:10) {
    gc()
  }
  
  # iterate over each split and merge with the demographics  [ it takes approximately 2 minutes using 8 threads and approximately 9 GB or RAM ]
  iterate_merg =  foreach::foreach(i = 1:LEN) %dopar% {
    
    # cat(paste0(i, '.'))
    
    x = blocks_subs_spl[[i]]
    
    # once we are in a specific trackt we split by the "fips_block_group"
    spl_blck = split(x, by = 'fips_block_group')
    
    # iterate and merge by 'fips_block_group'
    spl_blck_y = lapply(spl_blck, function(y) {
      
      # remove 'fips_tracks_id' column
      y$fips_tracks_id = NULL
      geom_union = sf::st_union(y$geometry) |>
        sf::st_as_sf()
      geom_union$fips_block_group = unique(y$fips_block_group)
      geom_union =   data.table::as.data.table(geom_union)
      colnames(geom_union) = c('geometry', 'fips_block_group')
      merg_geom = merge(x = geom_union, y = demographics_subs, by = 'fips_block_group') |>
        sf::st_as_sf()
      
      # make sure we have only POLYGON's
      merg_geom = sf::st_cast(merg_geom, 'POLYGON') |>
        data.table::as.data.table() |>
        suppressWarnings()
      merg_geom
    })
    
    spl_blck_y_bnd = data.table::rbindlist(spl_blck_y)
    spl_blck_y_bnd
  }
  
  
  if (.Platform$OS.type == "windows") {
    parallel::stopCluster(cl = cl)
  }
  
  # merge the sublists of the fips block groups
  iterate_merg_dtbl = data.table::rbindlist(iterate_merg)
  
  # iterate over the minute isochrones
  for (ISOCHR_min in names(dat_all_isochr)) {
    
    cat(glue::glue("{ISOCHR_min} will be processed ..."), '\n')
    
    # select sublist
    iso_chr_item = dat_all_isochr[[ISOCHR_min]]
    
    # we convert the fips data to an sf object
    iterate_merg_sf = sf::st_as_sf(iterate_merg_dtbl)
    
    # transform sf of the fips geometries
    geom_transf = sf::st_transform(x = iterate_merg_sf, crs = sf::st_crs(iso_chr_item))
    geom_transf = sf::st_make_valid(geom_transf)

    # first we have to observe if there is an intersection
    inters_geoms = sf::st_intersects(x = sf::st_geometry(geom_transf), y = sf::st_geometry(iso_chr_item), sparse = TRUE)
    inters_geoms_df = as.data.frame(inters_geoms) |>
      data.table::as.data.table()
    
    # we split by the fips 'row.id'
    inters_geoms_df_spl = split(inters_geoms_df, by = 'row.id')
    # length(inters_geoms_df_spl)
    
    # parallelization of the for-loop
    # OS specific code snippets
    if (.Platform$OS.type == "unix") {
      doParallel::registerDoParallel(cores = threads)
    }
    if (.Platform$OS.type == "windows") {
      cl = parallel::makePSOCKcluster(threads)
      doParallel::registerDoParallel(cl = cl)
    }
    
    # release ram before parallelization
    for (k in 1:10) {
      gc()
    }
    
    # we iterate and compute the proportion of females for the intersection between the geometries
    LEN_spl = length(inters_geoms_df_spl)
    
    # the same FIPS POLYGON can intersect with multiple isochrone MULTIPOLYGONS, that's why we receive more rows than initially for the input isochrones data (that means the output .shp files will have more rows)
    prop_inters = foreach::foreach(j = 1:LEN_spl) %dopar% {

      x = inters_geoms_df_spl[[j]]
      item_fips = geom_transf[unique(x$row.id), , drop = F]
      if (nrow(item_fips) != 1) stop("We expect only a single row for fips & population!")
      
      # for the isochrones perform a union because we might have more than 1 geometries
      item_isochr = iso_chr_item[x$col.id, , drop = F]
      
      # more than 1 isochrones can intersect with a fips
      item_res = sf::st_intersection(x = item_isochr, y = item_fips)
      if (nrow(item_res) != nrow(item_isochr)) stop("We expect the same number of rows for the input isochrones and the computed intersection so that we can add the proportions per row!")
      
      # percentage intersection
      area_inters = as.numeric(sf::st_area(item_res))
      area_fips = as.numeric(sf::st_area(item_fips))
      
      proportion = area_inters / area_fips
      item_isochr$intersected_area = round(x = proportion, digits = 4) * 100
      item_isochr$female_population = round(proportion * item_fips$population)
      item_isochr$fips_block_group = item_fips$fips_block_group
      item_isochr$initial_population = item_fips$population
      
      # # Verify visually
      # mp_fips = mapview::mapview(item_fips, legend = F, col.regions = 'blue', alpha.regions = 0.6)
      # mp_isoch = mapview::mapview(item_isochr, legend = F, col.regions = 'red', alpha.regions = 0.15)
      # mp_isoch + mp_fips
      
      data.table::as.data.table(item_isochr)
    }
    
    if (.Platform$OS.type == "windows") {
      parallel::stopCluster(cl = cl)
    }

    # rbind() and convert to an sf object
    prop_inters_sf = data.table::rbindlist(prop_inters) |>
      sf::st_as_sf()
    
    # write the data to an .shp file  
    file_isochr = file.path(DIR_FIPS, glue::glue("{ISOCHR_min}.shp"))
    sf::st_write(obj = prop_inters_sf, dsn = file_isochr)
  }
}


