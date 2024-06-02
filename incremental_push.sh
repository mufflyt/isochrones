#!/bin/bash

# List of files to add, commit, and push
files=(
    "data/08-get-block-group-overlap/combined_df.csv"
    
"data/08-get-block-group-overlap/isochrone_files/filtered_isochrones_30_minutes.shp"
    
"data/08-get-block-group-overlap/isochrone_files/filtered_isochrones_60_minutes.shp"
    
"data/08-get-block-group-overlap/isochrone_files/filtered_isochrones_120_minutes.shp"
    
"data/08-get-block-group-overlap/isochrone_files/filtered_isochrones_180_minutes.shp"
    "data/08-get-block-group-overlap/simplified/block_groups.shp"
)

# Loop through each file
for file in "${files[@]}"
do
    if git add -f "$file"; then
        echo "Added $file"
        if git commit -m "Add $file --no-verify"; then
            echo "Committed $file"
            if git push; then
                echo "Pushed $file"
            else
                echo "Failed to push $file"
                exit 1
            fi
        else
            echo "Failed to commit $file"
            exit 1
        fi
    else
        echo "Failed to add $file"
        exit 1
    fi
done

echo "All files have been processed successfully."
