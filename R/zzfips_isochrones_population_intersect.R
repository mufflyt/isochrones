
#N_fips_block_group represents the number of unique FIPS block groups present in a subset of data for each isochrone geometry

# required R packages
library(glue)
library(sf)
library(lwgeom)
library(data.table)
library(mapview)


# default directory (working directory assumed current)

# directory to save the results for each fips & minute-isochrones
dir_save = file.path(getwd(), 'data', 'save_fips_isochrones')
if (!dir.exists(dir_save)) stop(glue::glue("The directory '{dir_save}' does not exist!"))

# list the fips output directories
lst_dir = list.files(path = dir_save, full.names = F, include.dirs = T)
lst_dir

# select one of the fips directories
fips_dir = "01"

# full path to the selected fips directory
pth_fips_dir = file.path(dir_save, fips_dir)
if (!file.exists(pth_fips_dir)) stop(glue::glue("The directory '{pth_fips_dir}' does not exist!"))

# list the files of the output fips directory
fips_files = list.files(path = pth_fips_dir, pattern = '.shp$', full.names = F)
fips_files

# select one of the fips output .shp files
FILE = 'filtered_isochrones_120_minutes.shp'

fips_sel_shp = file.path(pth_fips_dir, FILE)
if (!file.exists(fips_sel_shp)) stop(glue::glue("The file '{fips_sel_shp}' does not exist!"))

# read the .shp file
dat_shp = sf::st_read(dsn = fips_sel_shp)
dat_shp

# print information about the geometries and the "fips_block_group" column
cat(glue::glue("The '{FILE}' file includes {length(unique(dat_shp$geometry))} unique geometries and {length(unique(dat_shp$fps_bl_))} unique fips_block_group in the total {nrow(dat_shp)} rows"), '\n')

# we create an additional text WKT column so that we can split the data
txt_wkt = lwgeom::st_astext(sf::st_geometry(dat_shp))

# we create a data.table with the relevant columns
relevant_cols = c("intrsc_", "fml_ppl", "fps_bl_", "intl_pp")

dtbl_subs = sf::st_drop_geometry(x = dat_shp)[, relevant_cols] |>
  data.table::as.data.table()

# we rename the columns because when saving to .shp these were abbreviated automatically
colnames(dtbl_subs) = c('intersected_area', 'female_population', 'fips_block_group', 'initial_population')

# we add the geometry column as wkt and split by this column
dtbl_subs$geometry = txt_wkt

dtbl_spl = split(dtbl_subs, by = 'geometry')
length(dtbl_spl)

# we have to iterate over each sublist:
# - remove duplicated rows
# - sum the total population that intersects with the isochrones using the fips codes
# - sum the female population that intersects with the isochrones using the fips codes
# - list all fips that intersect with the isochrone

aggreg_isochr_metad = lapply(seq_along(dtbl_spl), function(y) {
  cat(paste0(y, '.'))
  x = dtbl_spl[[y]]
  not_dups = which(!duplicated(x))
  x = x[not_dups, , drop = F]
  dtbl_x = list(N_fips_block_group = length(unique(x$fips_block_group)),
                INTERSECTED_female_population = sum(x$female_population, na.rm = T),
                TOTAL_population = sum(x$initial_population, na.rm = T),
                geometry = unique(x$geometry)) |>
    data.table::setDT()
  dtbl_x
}) |>
  data.table::rbindlist()

# convert to sf
isochr_dat = sf::st_as_sf(x = aggreg_isochr_metad, wkt = 'geometry', crs = sf::st_crs(dat_shp))
  
# visualization of all isochrones 
mp = mapview::mapview(isochr_dat, zcol = 'TOTAL_population', layer.name = 'TOTAL_population')
mp

# visualization where each isochrone is a different layer in the toggle bar on the top left of leaflet, so that they can be opt out
dat_layers = lapply(1:nrow(isochr_dat), function(x) isochr_dat[x, , drop = F])
mp_layers = mapview::mapview(dat_layers, legend = F)
mp_layers

