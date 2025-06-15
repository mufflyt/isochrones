#............................................................................................................................................................................... info task
# I am working on how to find when a physician retires or dies and translate that into the year they stopped working. I think we have three
# sources for this. 

# 1st. we have the NPI deactivation table: https://download.cms.gov/nppes/NPPESDeactivatedNPIReport040824.zip. The file
# is attached. This matches NPI to NPI; a date is given for when the NPI number was deactivated. 

# 2nd. approach is when the patient does not appear in the next year's NPI database. I downloaded the NPI database every year, so it is a duplicate database instead of the
# deduplicated one from NBER. The file is called end_sp_duckdb_npi_all.csv and is attached.

# 3rd. approach is a goba file (goba_unique_goba_deceased_retired.csv) where there is a "certStatus" %in% c("Deceased Diplomate", "Retired"). This requires 
# a first name and last name left join and does not include the year that the physician died or retired but knowing the status would 
# be helpful. I included some code that does not work but is an idea of where I want to go: retirement.R.
#...............................................................................................................................................................................

# required R packages
if(!require(pacman)) install.packages("pacman");
pacman::p_load(here,
               data.table,
               parallel,
               glue,
               lubridate,
               # fuzzyjoin,
               zoomerjoin,
               install = TRUE)

# default directory
def_dir = here::here()
def_dir

data_dir = file.path(def_dir, 'data_retirement')
if (!dir.exists(data_dir)) stop(glue::glue("The directory '{data_dir}' does not exist!"))

your_data_frame = data.table::fread(file = file.path(data_dir, "processed_nips.csv"), header = TRUE, nThread = parallel::detectCores())
retirement_data_frame = data.table::fread(file = file.path(data_dir, "retirement_NPPES Deactivated NPI Report 20240408.csv"), header = TRUE, nThread = parallel::detectCores())
duckdb_npi_all = data.table::fread(file = file.path(data_dir, 'end_sp_duckdb_npi_all.csv'), header = TRUE, nThread = parallel::detectCores())
goba_df = data.table::fread(file = file.path(data_dir, 'goba_unique_goba_deceased_retired.csv'), header = TRUE, nThread = parallel::detectCores())
phys_medic = data.table::fread(file = file.path(data_dir, 'physician_compare_retirement_years.csv'), header = TRUE, nThread = parallel::detectCores())

#........................................................................................................... 1st. approach

# convert the deactivation date column names to lower case
colnames(retirement_data_frame) = tolower(colnames(retirement_data_frame))

LEN_npi_unq_ret = length(unique(retirement_data_frame$npi))
if (LEN_npi_unq_ret != nrow(retirement_data_frame)) stop(glue::glue("The unique NIPS (of the retirement_NPPES Deactivated data) are {LEN_npi_unq_ret} whereas the number of rows are {nrow(retirement_data_frame)}"))

LEN_npi_unq_proc = length(unique(your_data_frame$npi))
if (LEN_npi_unq_proc != nrow(your_data_frame)) message(glue::glue("The unique NIPS (of the processed data) are {LEN_npi_unq_proc} whereas the number of rows are {nrow(your_data_frame)}"))

# from the "your_data_frame" data.frame
# - we'll use the 'data_exist' column and we'll keep only the TRUE cases
# - we'll split the data by the NIPS id's 
# - from each sublist we'll keep the most recent year from the "lastupdatestr" column

df_subs = subset(your_data_frame, data_exist == TRUE)
df_subs_spl = split(df_subs, by = 'npi')
df_keep_recent = lapply(df_subs_spl, function(x) {
  x = x[order(x$lastupdatestr, decreasing = TRUE), ]
  x = x[1, , drop = F]
  x
}) |>
  data.table::rbindlist()

# order by NPI in decreasing order
df_keep_recent = df_keep_recent[order(df_keep_recent$npi, decreasing = F), ]

# we check for NA's in the retirement_data_frame'
if (any(colSums(is.na(retirement_data_frame)) > 0)) message("There are missing values in the 'retirement_data_frame'!")

# we convert the "nppes deactivation date" column to a Date object (from the current which is character)
retirement_data_frame$`nppes deactivation date` = lubridate::mdy(retirement_data_frame$`nppes deactivation date`)

# we'll create a column named as "deactivation_year" in the 'retirement_data_frame'
retirement_data_frame$deactivation_year = lubridate::year(retirement_data_frame$`nppes deactivation date`)

# We omit the nppes deactivation date
retirement_data_frame$`nppes deactivation date` = NULL

# we'll merge the data by the 'npi' column
merg_npi = merge(x = df_keep_recent, y = retirement_data_frame, by = 'npi', all.x = TRUE)

# #........................................................................................................... 2nd. approach (not used as NPI's same as for "your_data_frame")
#
# # follow the same approach as we did for the 'your_data_frame' (see the previous comments)
# duckdb_npi_all_spl = split(duckdb_npi_all, by = 'npi')
# df_npi_all = lapply(duckdb_npi_all_spl, function(x) {
#   x = x[order(x$lastupdatestr, decreasing = T), ]
#   x = x[1, , drop = F]
#   x
# }) |>
#   data.table::rbindlist()
#
# # order by NPI in decreasing order
# df_npi_all = df_npi_all[order(df_npi_all$npi, decreasing = F), ]
#
# # check if we have differences in the NPI's between the 2 data.frames (no differences)
# length(setdiff(x = df_keep_recent$npi, y = df_npi_all$npi))
#
# # check if all NPI (after sorting) are the same
# all.equal(df_keep_recent$npi, df_npi_all$npi) & (length(df_keep_recent$npi) == length(df_npi_all$npi))
#
# # check that the years are the same for both data.frames
# all.equal(df_keep_recent$lastupdatestr, df_npi_all$lastupdatestr)

#........................................................................................................... 3rd. approach  (fuzzy string matching)

# the number of unique ID's of the 'goba' data.frame
length(unique(goba_df$userid))

# We'll keep specific columns from the 'goba' data.frame
cols_goba_keep = c('name', 'startDate', 'certStatus')
goba_df_subs = goba_df[, ..cols_goba_keep]

# adjust the names of the goba columns
colnames(goba_df_subs) = glue::glue("{colnames(goba_df_subs)}_goba")

# we'll convert to lower case the 'name_goba' column
goba_df_subs$name_goba = tolower(goba_df_subs$name_goba)

# we'll create a new column for the 'df_keep_recent' data.frame using the 'pfname' and 'plname' columns named as 'name_goba'  and we'll convert to lower case
df_keep_recent$name_goba = tolower(glue::glue("{df_keep_recent$pfname} {df_keep_recent$plname}"))

set.seed(1)
join_out = zoomerjoin::jaccard_left_join(a = df_keep_recent,
                                         b = goba_df_subs,
                                         by = "name_goba",
                                         n_gram_width = 6,      # The length of the n_grams used in calculating the Jaccard similarity
                                         n_bands = 350,          # The number of bands used in the minihash algorithm
                                         band_width = 11,        # The length of each band used in the minihashing algorithm (default is 8)
                                         threshold = 0.6,        # The Jaccard similarity threshold above which two strings should be considered a match
                                         progress = FALSE)        # print progress

join_out_dtbl = data.table::as.data.table(join_out)
join_out_dtbl
# colSums(is.na(join_out_dtbl))

#............................................................ Gives segfault due to RAM limits
# #perform fuzzy matching left join
# jn_fuz = fuzzyjoin::stringdist_join(x = df_keep_recent,
#                                     y = goba_df_subs,
#                                     by = 'name_goba',              #match based on name_goba
#                                     mode = 'left',           #use inner join
#                                     method = "jw",          #use jw distance metric
#                                     max_dist = 99,
#                                     distance_col='dist')
#   # group_by(team.x) %>%
#   # slice_min(order_by=dist, n=1)
#
# # joined_dists <- sub_misspellings %>%
# #   stringdist_inner_join(words, by = c(misspelling = "word"), max_dist = 2,
# #                         distance_col = "distance")
#
#........................................................................................................... 4th. approach ('Medicare' data)

# convert the deactivation date column names to lower case
colnames(phys_medic) = tolower(colnames(phys_medic))

LEN_npi_unq_medic = length(unique(phys_medic$npi))
if (LEN_npi_unq_medic != nrow(phys_medic)) stop(glue::glue("The unique NIPS (of the physician Medicare data) are {LEN_npi_unq_medic} whereas the number of rows are {nrow(phys_medic)}"))

# we'll merge the data by the 'npi' column
merg_medic = merge(x = df_keep_recent, y = phys_medic, by = 'npi', all.x = TRUE)

not_isnan_medic = which(!is.na(merg_medic$`retirement year`))
length(not_isnan_medic)

# add also a column specifying the data source
merg_medic$data_source = NA_character_

#============================================================================================================== Populate the "your_data_frame" based on the previous 3 approaches which returned results

# Preference will be given in the following way:
#
# 1st. Medicare data
# 2nd. retirement_NPPES Deactivated NPI Report
# 3rd. 'zoomerjoin' R package and fuzzy string matching
#.......................................................

# 1st. Medicare data (already computed previously see "merg_medic")

# 2nd. retirement_NPPES Deactivated NPI Report
# we exclude the intersection with the "Medicare data"

not_isnan_retir = which(!is.na(merg_npi$deactivation_year))
length(not_isnan_retir)

# update the data source
merg_medic$data_source[not_isnan_medic] = 'Medicare'

# compute the remaining for the retirement NPPES
remain_retir = setdiff(x = not_isnan_retir, y = not_isnan_medic)
# length(remain_retir)

# verify that we have NA's for these cases
if (sum(is.na(merg_medic$`retirement year`[remain_retir])) != length(remain_retir)) stop("We expect that the indices that differ have all NA's!")

# we add the difference of the 'deactivation_year' as well
merg_medic$`retirement year`[remain_retir] = merg_npi$deactivation_year[remain_retir]
# sum(!is.na(merg_medic$`retirement year`))  # 15565 + 2296

# update the data source
merg_medic$data_source[remain_retir] = 'retirement_NPPES Deactivated'
# sum(!is.na(merg_medic$data_source))

# 3rd. 'zoomerjoin' R package and fuzzy string matching
subs_fuz = subset(join_out_dtbl, !is.na(certStatus_goba))
length(unique(subs_fuz$npi))

# remove NPI's which have duplicated rows because this means that the same name matched multiple cases
subs_fuz_spl = split(subs_fuz, by = 'npi')
idx_dups = as.vector(which(unlist(lapply(subs_fuz_spl, nrow)) != 1))

subs_fuz_spl_upd = subs_fuz_spl[-idx_dups] |>
  data.table::rbindlist()
# sum(duplicated(subs_fuz_spl_upd$npi))

idx_fuz_npi = which(merg_medic$npi %in% subs_fuz_spl_upd$npi)
# length(idx_fuz_npi)

idx_replace = which(is.na(merg_medic$`retirement year`[idx_fuz_npi]))
idx_fuz_replace = idx_fuz_npi[idx_replace]
# sum(is.na(merg_medic$`retirement year`[idx_fuz_replace]))       # verify

# convert to character so that I can add the 'certStatus_goba' observations
merg_medic$`retirement year` = as.character(merg_medic$`retirement year`)

# we add the difference of the 'deactivation_year' as well
merg_medic$`retirement year`[idx_fuz_replace] = as.character(subs_fuz_spl_upd$startDate_goba[idx_replace])
# sum(!is.na(merg_medic$`retirement year`))  # 15565 + 2296 + 68

# update the data source
merg_medic$data_source[idx_fuz_replace] = 'fuzzy matching goba'

# keep subset of columns
subs_cols_medic = c('npi', 'retirement year', 'data_source')
merg_medic_subs = merg_medic[, ..subs_cols_medic]

# rename the column to "year_of_retirement"
colnames(merg_medic_subs) = c('npi', 'year_of_retirement', 'data_source')

# merge with the initial "your_data_frame" which includes the duplicated NPI's and keep the initial column names order
set_colnams = c(colnames(your_data_frame), 'year_of_retirement', 'data_source')
merge_init = merge(x = your_data_frame, y = merg_medic_subs, by = 'npi')
merge_init = merge_init[, ..set_colnams]

# write the output to a .csv file
data.table::fwrite(x = merge_init, file = file.path(data_dir, 'updated_processed_nips.csv'), row.names = FALSE)

