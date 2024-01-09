library(tidyverse)
library(sf)
library(easyr)
library(hereR)
library(data.table)

Sys.setenv(HERE_API_KEY = "VnDX-Rafqchcmb4LUDgEpYlvk8S1-LCYkkrtb1ujOrM")
readRenviron("~/.Renviron")
hereR::set_key("VnDX-Rafqchcmb4LUDgEpYlvk8S1-LCYkkrtb1ujOrM")

input_file <- readr::read_csv("data/inner_join_postmastr_clinician_data.csv") %>%
  dplyr::mutate(id = row_number()) %>%
  dplyr::filter(postmastr.name.x != "Hye In Park, MD") #Not able to create isochrone for some reason

input_file$lat <- as.numeric(input_file$lat)
input_file$long <- as.numeric(input_file$long)

input_file_sf <- input_file %>%
  st_as_sf(coords = c("long", "lat"), crs = 4326) #%>%
  #slice(200:400)
  #tail(300)

posix_time <- (as.POSIXct("2023-10-20 09:00:00", format = "%Y-%m-%d %H:%M:%S"))

isochrones_data <- hereR::isoline(poi = input_file_sf,
                           range = c(1800, 3600, 7200, 10800),
                           #range = c(1),
                             datetime = posix_time, #POSIXct object, datetime for the departure
                             routing_mode = "fast", #Try to route fastest route or "short"est route.
                             range_type = "time", # character of the isolines: "distance" or "time"
                             transport_mode = "car", #specified for "car" transport instead of "truck" or "pedestrian"
                             url_only = FALSE,
                             optimize = "balanced",
                             traffic = TRUE, # Includes real-time traffic
                             aggregate = FALSE); isochrones_data
dim(isochrones_data)


if (requireNamespace("mapview", quietly = TRUE)) {
  mapview::mapview(isochrones_data,
                   col.regions = "red",
                   map.types = c("Esri.WorldTopoMap"),
                   legend = FALSE,
                   homebutton = FALSE
  )
}

input_file_ids <- unique(input_file_sf$id)
isochrone_ids <- unique(isochrones_data$id)
missing_ids <- input_file_ids[!input_file_ids %in% isochrone_ids]
missing_rows <- input_file[input_file$id %in% missing_ids, ]; View(missing_rows)

isochrones_data <- isochrones_data %>% # Rename all columns with a prefix fo "here."
  rename_all(~paste0("here.", .))

write_csv(isochrones_data, "data/isochrones_data_12_10_2023.csv")

finally_isochrones <- input_file %>%
  left_join(`isochrones_data`, by = join_by(`id` == `here.id`))

class(finally_isochrones)
write_rds(finally_isochrones, "data/finally_isochrones.rds")
st_write(finally_isochrones, "data/finally_isochrones.shp", append = TRUE)

View(finally_isochrones)

#### Example of geocoding
# df <- data.frame(
#   company = c("Schweizerische Bundesbahnen SBB", "Bahnhof AG", "Deutsche Bahn AG"),
#   address = c("Wylerstrasse 123, 3000 Bern 65", "not_an_address", "Potsdamer Platz 2, 10785 Berlin"),
#   stringsAsFactors = FALSE
# )
#
#
# locs <- geocode(df$address)
# geocoded_sfdf <- st_as_sf(data.frame(locs, df[locs$id, ]))
#
# #### This works!
# # Load the required libraries
# library(hereR)
# library(sf)
#
# # Example of geocoding
# df <- data.frame(
#   company = c("Schweizerische Bundesbahnen SBB", "Bahnhof AG", "Deutsche Bahn AG"),
#   address = c("Wylerstrasse 123, 3000 Bern 65", "not_an_address", "Potsdamer Platz 2, 10785 Berlin"),
#   stringsAsFactors = FALSE
# )
#
# locs <- geocode(df$address)
#
# # Filter out the geocoded addresses with valid results
# valid_locs <- locs
# class(valid_locs)
#
# # Create an sf object from the valid geocoded addresses
# valid_sf <- st_as_sf(data.frame(valid_locs, df[valid_locs$id, ]))
#
# # Calculate isochrones
# isochrones <- isoline(poi = valid_sf, range = seq(5, 30, 5) * 60)


# Validate the file of geocoded data.
input_file <- readr::read_csv("data/isochrones/inner_join_postmastr_clinician_data.csv") %>%
  dplyr::mutate(id = row_number()) %>%
  dplyr::filter(postmastr.name.x != "Hye In Park, MD") %>% #Not able to create isochrone for some reason
  head(52)


library(tidyverse)
library(hereR)

test_and_process_isochrones <- function(input_file) {
  input_file <- input_file %>%
    mutate(id = row_number()) %>%
    filter(postmastr.name.x != "Hye In Park, MD")

  input_file$lat <- as.numeric(input_file$lat)
  input_file$long <- as.numeric(input_file$long)

  input_file_sf <- input_file %>%
    st_as_sf(coords = c("long", "lat"), crs = 4326)

  posix_time <- as.POSIXct("2023-10-20 09:00:00", format = "%Y-%m-%d %H:%M:%S")

  error_rows <- vector("list", length = nrow(input_file_sf))

  for (i in 1:nrow(input_file_sf)) {
    row_data <- input_file_sf[i, ]

    isochrones <- tryCatch(
      {
        hereR::isoline(
          poi = row_data,
          range = c(1),
          datetime = posix_time,
          routing_mode = "fast",
          range_type = "time",
          transport_mode = "car",
          url_only = FALSE,
          optimize = "balanced",
          traffic = TRUE,
          aggregate = FALSE
        )
      },
      error = function(e) {
        message("Error processing row ", i, ": ", e$message)
        return(NULL)
      }
    )

    if (is.null(isochrones)) {
      error_rows[[i]] <- i
    }
  }

  # Collect the rows that caused errors
  error_rows <- unlist(error_rows, use.names = FALSE)

  if (length(error_rows) > 0) {
    message("Rows with errors: ", paste(error_rows, collapse = ", "))
  } else {
    message("No errors found.")
  }
}


##### To do the actual gathering of the isochrones: `process_and_save_isochrones`.  We do this in chunks of 25 because we were losing the entire searched isochrones when one error hit.  There is a 1:1 relationship between isochrones and rows in the `input_file` so to match exactly on row we need no errors.  Lastly, we save as a shapefile so that we can keep the MULTIPOLYGON geometry setting for the sf object making it easier to work with the spatial data in the future for plotting, etc.  I struggled because outputing the data as a dataframe was not easy to write it back to a MULTIPOLYGON.
library(tidyverse)
library(sf)
library(easyr)
library(hereR)
library(data.table)

Sys.setenv(HERE_API_KEY = "VnDX-Rafqchcmb4LUDgEpYlvk8S1-LCYkkrtb1ujOrM")
readRenviron("~/.Renviron")
hereR::set_key("VnDX-Rafqchcmb4LUDgEpYlvk8S1-LCYkkrtb1ujOrM")

input_file <- readr::read_csv("data/inner_join_postmastr_clinician_data.csv") %>%
  dplyr::mutate(id = row_number()) %>%
  dplyr::filter(postmastr.name.x != "Hye In Park, MD") #Not able to create isochrone for some reason

# Filter out the rows that are going to error out after using the test_and_process_isochrones function.
error_rows <- c(265, 431, 816, 922, 1605, 2049, 2212, 2284, 2308, 2409, 2482, 2735, 2875, 2880, 3150, 3552, 3718)

input_file_no_error_rows <- input_file %>%
  dplyr::filter(!id %in% error_rows)

process_and_save_isochrones <- function(input_file, chunk_size = 25) {
  input_file <- input_file %>%
    dplyr::mutate(id = dplyr::row_number()) %>%
    dplyr::filter(postmastr.name.x != "Hye In Park, MD")

  input_file$lat <- as.numeric(input_file$lat)
  input_file$long <- as.numeric(input_file$long)

  input_file_sf <- input_file %>%
    sf::st_as_sf(coords = c("long", "lat"), crs = 4326)

  posix_time <- as.POSIXct("2023-10-20 09:00:00", format = "%Y-%m-%d %H:%M:%S")

  num_chunks <- ceiling(nrow(input_file_sf) / chunk_size)
  isochrones_list <- list()

  for (i in 1:num_chunks) {
    start_idx <- (i - 1) * chunk_size + 1
    end_idx <- min(i * chunk_size, nrow(input_file_sf))
    chunk_data <- input_file_sf[start_idx:end_idx, ]

    isochrones <- tryCatch(
      {
        hereR::isoline(
          poi = chunk_data,
          range = c(1800, 3600, 7200, 10800),
          datetime = posix_time,
          routing_mode = "fast",
          range_type = "time",
          transport_mode = "car",
          url_only = FALSE,
          optimize = "balanced",
          traffic = TRUE,
          aggregate = FALSE
        )
      },
      error = function(e) {
        message("Error processing chunk ", i, ": ", e$message)
        return(NULL)
      }
    )

    if (!is.null(isochrones)) {
      # Create the file name with the current date and time
      current_datetime <- format(Sys.time(), "%Y%m%d%H%M%S")

      file_name <- paste("data/isochrones/isochrones_", current_datetime, "_chunk_", min(chunk_data$id), "_to_", max(chunk_data$id))

      # Assuming "arrival" field is originally in character format with both date and time
      # Convert it to a DateTime object
      isochrones$arrival <- as.POSIXct(isochrones$arrival, format = "%Y-%m-%d %H:%M:%S")

      # Save the data as a shapefile with the layer name "isochrones"
      sf::st_write(
        isochrones,
        dsn = file_name,
        layer = "isochrones",
        driver = "ESRI Shapefile",
        quiet = FALSE
      )

      # Store the isochrones in the list
      isochrones_list[[i]] <- isochrones
    }
  }

  # Combine all isochrones from the list into one data frame
  isochrones_data <- do.call(rbind, isochrones_list)

  return(isochrones_data)
}

# Call the function with your input_file
isochrones_sf <- process_and_save_isochrones(input_file_no_error_rows)

# Check the dimensions of the final isochrones_data
dim(isochrones_sf)
class(isochrones_sf)

sf::st_write(
  isochrones_sf,
  dsn = "data/isochrones/isochrones_all_combined",
  layer = "isochrones",
  driver = "ESRI Shapefile",
  quiet = FALSE)
