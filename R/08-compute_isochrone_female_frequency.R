
# get the number of women for each isochrone.

# required R packages
library(glue)
library(sf)
library(dplyr)
library(mapview)
library(lwgeom)
library(data.table)

# assuming that the working directory is the one where the 'data' is saved
setwd(dir = getwd())

#................................................................................. the .csv files do not include the gender column
# pth_dat = file.path(getwd(), "data/08-get-block-group-overlap/combined_df.csv")
# combined_df = readr::read_csv(pth_dat)
# spec(x = combined_df)
#................................................................................. the .shp files include the "pgender" column

mins180 <- st_read("data/08-get-block-group-overlap/isochrone_files/filtered_isochrones_180_minutes.shp")
names(mins180)
head(mins180, 2)
mins180$pgender
plot(mins180[16])

# directory of the .shp files
dir_shp = file.path(getwd(),'data', '08-get-block-group-overlap', 'isochrone_files')
if (!dir.exists(dir_shp)) stop(glue::glue("The directory '{dir_shp}' does not exist!"))

# list the updated .shp files of the "data/08-get-block-group-overlap/isochrone_files/" directory
lst_shp = list.files(path = dir_shp, pattern = ".shp$", full.names = FALSE)
if (length(lst_shp) == 0) stop("The directory does not include any .shp files!")

# we expect that the files start with "filtered_isochrones_*"
idx_shp = which(stringr::str_detect(string = lst_shp, pattern = '^filtered_isochrones'))
if (length(idx_shp) == 0) stop("We expect that the files start with 'filtered_isochrones_*'!")
if (length(lst_shp) != length(idx_shp)) stop("We expect that all files start with 'filtered_isochrones_*'!")

dat_all_isochr = lapply(seq_along(lst_shp), function(x) {
  file_x = lst_shp[x]
  dat_x = sf::st_read(file.path(dir_shp, file_x), quiet = TRUE)
  cat(glue::glue("Missing values in the 'pgender' column of the {unique(dat_x$ischrn__)} minutes isochrones:  {sum(is.na(dat_x$pgender))}"), '\n')
  dat_x
})

# Missing values in the 'pgender' column of the 120 minutes isochrones:  0 
# Missing values in the 'pgender' column of the 180 minutes isochrones:  0 
# Missing values in the 'pgender' column of the 30 minutes isochrones:  0 
# Missing values in the 'pgender' column of the 60 minutes isochrones:  0

# we bind all rows of the c(30, 60, 120, 180) isochrones
dat_all_isochr_df = dplyr::bind_rows(dat_all_isochr)

# verify the unique genders
print(unique(dat_all_isochr_df$pgender))

# we subset and keep only the females
dat_isochr_fem = subset(dat_all_isochr_df, pgender == 'Female')

nrow(dat_isochr_fem)
length(unique(dat_isochr_fem$geometry))

# # verify that the geometries are isochrones
# mp = mapview::mapview(sf::st_geometry(dat_isochr_fem[1, ]))
# mp

# get the geometry column as a character string that will be used as ID in the initial data as well
geom_isochr = sf::st_geometry(dat_isochr_fem) |>
  lwgeom::st_astext()

# add the ID to the initial data
dat_isochr_fem$ID_ISOCHRONES = geom_isochr

# create a data.table from the geometry
dtbl_isochr_fem = list(ID_ISOCHRONES = geom_isochr) |>
  data.table::setDT()

# compute the frequency of females per isochrone
dtbl_isochr_fem_freq = dtbl_isochr_fem[, .(female_freq = .N), by = 'ID_ISOCHRONES']

# observe the female frequency per isochrone
table(dtbl_isochr_fem_freq$female_freq)

# find out if we have empty geometries
geom_empty = which(sf::st_is_empty(dat_isochr_fem))
if (length(geom_empty) > 0) {
  message(glue::glue("There are {length(geom_empty)} empty geometries (or isochrones) in the data!"), '\n')
}

# convert the sf object to a data.table
dat_isochr_fem_dtbl = data.table::as.data.table(dat_isochr_fem)

# merge by the 'ID_ISOCHRONES' to include the frequency of the females per Isochrone
merg_isochr_fem = merge(x = dat_isochr_fem_dtbl, y = dtbl_isochr_fem_freq, by = 'ID_ISOCHRONES')

# we remove the 'ID_ISOCHRONES' as it is no longer required
merg_isochr_fem$ID_ISOCHRONES = NULL

# We convert to an sf object
merg_isochr_fem_sf = sf::st_as_sf(merg_isochr_fem)

# verify the female frequency per isochrone in the final data
table(merg_isochr_fem_sf$female_freq)

# write the output .shp file
sf::st_write(obj = merg_isochr_fem_sf, dsn = file.path(dir_shp, 'female_frequency_per_isochrone.shp'))
