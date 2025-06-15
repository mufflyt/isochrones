# Load libraries
library(RsimMosaic)
library(magick)
library(purrr)
library(jpeg)
gc()

# Define paths
base_path <- "/Volumes/Video Projects Muffly 1/Workforce/Hydrology/"
tiles_raw <- "/Volumes/Video Projects Muffly 1/Workforce/Hydrology/iCloud Photos/"
tiles_filtered <- file.path(base_path, "tiles_filtered")
tiles_ready <- file.path(base_path, "tiles_ready")
dir.create(tiles_filtered, showWarnings = FALSE)
dir.create(tiles_ready, showWarnings = FALSE)

target_image_path <- file.path(base_path, "wife2.png")
resized_target_path <- file.path(base_path, "wife_resized.jpg")

base_path <- sub("/$", "", base_path)  # Remove trailing slash if present
output_mosaic_path <- file.path(base_path, paste0("wife_mosaic_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".jpg"))

# Convert HEIC to JPEG
heic_files <- list.files(tiles_raw, pattern = "\\.HEIC$", full.names = TRUE)
# walk(heic_files, function(path) {
#   img <- magick::image_read(path)
#   jpg_path <- sub("\\.HEIC$", ".jpg", path, ignore.case = TRUE)
#   magick::image_write(img, jpg_path, format = "jpeg")
# })
# file.remove(heic_files)
# invisible(gc())

library(magick)
library(purrr)

heic_files <- list.files(tiles_raw, pattern = "\\.HEIC$", full.names = TRUE)

for (path in heic_files) {
  tryCatch({
    message("Processing: ", basename(path))
    
    img <- magick::image_read(path)
    
    jpg_path <- sub("\\.HEIC$", ".jpg", path, ignore.case = TRUE)
    magick::image_write(img, jpg_path, format = "jpeg")
    
    rm(img)
    gc()
    
  }, error = function(e) {
    message("Failed: ", basename(path), " — ", e$message)
  })
}

# Optionally remove HEICs afterward
file.remove(heic_files)


# Filter out small or unreadable JPEGs
jpeg_files <- list.files(tiles_raw, pattern = "\\.jpg$", full.names = TRUE, ignore.case = TRUE)
walk(jpeg_files, function(f) {
  tryCatch({
    img <- jpeg::readJPEG(f)
    dims <- dim(img)
    if (dims[1] >= 60 && dims[2] >= 60) {
      file.copy(f, file.path(tiles_filtered, basename(f)))
    } else {
      message("Skipped small image: ", f)
    }
  }, error = function(e) {
    message("Skipped invalid image: ", f)
  })
})
invisible(gc())

# Resize tile images to 60x60 with white background
tile_files <- list.files(tiles_filtered, pattern = "\\.jpg$", full.names = TRUE)
good_tiles <- 0

# Process in batches of 10 to manage memory
# Change this variable to set your desired tile size
tile_size <- 200  # Change this to any size you want (e.g., 80, 120, 150)

for (i in seq(1, length(tile_files), by = 10)) {
  batch_end <- min(i + 9, length(tile_files))
  batch_files <- tile_files[i:batch_end]
  
  walk(batch_files, function(f) {
    tryCatch({
      img <- magick::image_read(f)
      info <- magick::image_info(img)
      if (info$width >= tile_size && info$height >= tile_size) {  # Check against new size
        img_resized <- magick::image_resize(img, paste0(tile_size, "x", tile_size, "!"))  # New size
        img_padded <- magick::image_background(img_resized, "white")
        out_path <- file.path(tiles_ready, sprintf("tile_%04d.jpg", good_tiles + 1))
        magick::image_write(img_padded, out_path, format = "jpeg", quality = 95)
        good_tiles <<- good_tiles + 1
        
        # Clean up image objects immediately
        rm(img_resized, img_padded)
      }
      rm(img)  # Always clean up the original
    }, error = function(e) {
      message("Error processing: ", basename(f))
    })
  })
  
  # Force garbage collection after each batch
  gc(full = TRUE)
  
  # Progress update
  if (i %% 50 == 1) {
    message("Processed ", batch_end, " of ", length(tile_files), " files...")
  }
}
invisible(gc())
message("Total valid tile images: ", good_tiles)

#### Pre-screen and remove invalid or grayscale tiles before mosaic
library(jpeg)
library(purrr)

tile_files <- list.files(tiles_ready, pattern = "\\.jpg$", full.names = TRUE)
valid_tiles <- character()

for (f in tile_files) {
  tryCatch({
    img <- jpeg::readJPEG(f)
    if (length(dim(img)) == 3 && dim(img)[3] == 3) {
      valid_tiles <- c(valid_tiles, f)
    } else {
      message("Skipping grayscale or invalid image: ", basename(f))
      file.remove(f)
    }
  }, error = function(e) {
    message("Removing unreadable image: ", basename(f))
    file.remove(f)
  })
}

message(length(valid_tiles), " tile images are valid and RGB.")


# Resize target image if too large
target_img <- magick::image_read(target_image_path)
img_info <- magick::image_info(target_img)
if (img_info$width > 150 || img_info$height > 150) {
  message("Resizing target image...")
  target_img <- magick::image_resize(target_img, "150x150")
}
magick::image_write(target_img, resized_target_path, format = "jpeg")
invisible(gc())

# Create the mosaic
message("Creating mosaic at: ", output_mosaic_path)
RsimMosaic::composeMosaicFromImageRandom(
  originalImageFileName = resized_target_path,
  outputImageFileName = output_mosaic_path,
  imagesToUseInMosaic = tiles_ready,
  useGradients = TRUE,
  removeTiles = TRUE,
  fracLibSizeThreshold = 0.8,
  repFracSize = 0.2,
  verbose = TRUE
)

invisible(gc())

# Open the result
if (file.exists(output_mosaic_path)) {
  utils::browseURL(output_mosaic_path)
} else {
  message("Mosaic creation failed.")
}
invisible(gc())


###
library(magick)

# Paths
mosaic_path <- "/Volumes/Video Projects Muffly 1/Workforce/Hydrology/wife_mosaic_20250510_222504.jpg"
original_path <- "/Volumes/Video Projects Muffly 1/Workforce/Hydrology/wife2.png"
output_path <- "/Volumes/Video Projects Muffly 1/Workforce/Hydrology/wife_mosaic_blended.jpg"

mosaic <- magick::image_read(mosaic_path)
original <- magick::image_read(original_path)

# Resize original to match mosaic
mosaic_info <- magick::image_info(mosaic)
original_resized <- magick::image_resize(
  original, 
  geometry = paste0(mosaic_info$width, "x", mosaic_info$height, "!")
)

# Decrease brightness & increase transparency by blending, not whitening
original_faded <- magick::image_modulate(original_resized, brightness = 90, saturation = 95)
blended <- magick::image_composite(mosaic, original_faded, operator = "blend", compose_args = "20")

# Save result
magick::image_write(blended, output_path)
utils::browseURL(output_path)

# Export for Large-Format Printing
high_res <- magick::image_scale(blended, "4800x")  # 300 DPI × 16 inches
magick::image_write(high_res, "/Volumes/Video Projects Muffly 1/Workforce/Hydrology/wife_mosaic_print_ready.jpg", quality = 100)

#### R Code to Create High-Resolution 36×48" Mosaic
library(magick)

# Read your existing mosaic
mosaic <- magick::image_read("/Volumes/Video Projects Muffly 1/Workforce/Hydrology/wife_mosaic_blended.jpg")

# Resize to 10,800 x 14,400 pixels for 36x48 inches @ 300 DPI
mosaic_print <- magick::image_resize(mosaic, "10800x14400!")

# Save at high quality
magick::image_write(mosaic_print, "/Volumes/Video Projects Muffly 1/Workforce/Hydrology/wife_mosaic_36x48_300dpi.jpg", quality = 100)
