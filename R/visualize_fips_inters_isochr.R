
library(sf)
library(mapview)
library(data.table)

# path to one of the saved .shp files
pth = 'data/save_fips_isochrones/01/filtered_isochrones_120_minutes.shp'
dat = sf::st_read(pth)
dat

# we keep the relevant columns
keep_cols = c('intrsc_', 'fml_ppl', 'fps_bl_', 'intl_pp')

# we convert to data.table
dat_subs = dat[, keep_cols] |>
  data.table::as.data.table()

# we rename the columns
colnames(dat_subs) = c('intersected_area', 'female_population', 'fips_block_group', 'initial_population', 'geometry')
dat_subs

# we split by fips_block_group
dat_subs_spl = split(dat_subs, by = 'fips_block_group')
length(dat_subs_spl)
summary(unlist(lapply(dat_subs_spl, nrow)))

# we select one of the sublists
idx = 1

dat_idx = dat_subs_spl[[1]] |>
  sf::st_as_sf()
dat_idx

# we visualize the selected fips
mp = mapview::mapview(dat_idx, zcol = 'intersected_area', layer.name = 'intersected_area', alpha.regions = 0.2)
mp


