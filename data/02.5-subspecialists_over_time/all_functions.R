
# Step 1 - scrape all obgyn details url from search result for each state
collect_obgyn_details_urls <- function(state_obgyn_url){
  
  remDr$navigate(state_obgyn_url)
  
  # <a href="/usearch?what=Obstetrics%20%26%20Gynecology&amp;entityCode=PS574&amp;where=richmond%20hill%2C%20on&amp;pt=43.88%2C-79.42&amp;pageNum=14&amp;sort.provider=bestmatch&amp;=" class="s8oem JQ+PU" aria-label="Last Page, Page 14" data-qa-target="pagination--page-14 current-page last-page" aria-current="page"><span>14</span></a>
  preferred_class <- "current-page last-page"
  obgyn_hrefs <- data.frame()  # Initialize an empty data frame
  
  pagination_next <- tryCatch(
    remDr$findElement(using = "xpath", value = "//a[@data-qa-target='pagination--next-page']"),
    error = function(e) NULL
  )
  
  if (is.null(pagination_next)) {
    
    # Select all anchor tags within a specific element (e.g., class="card-name")
    anchor_tags <- remDr$findElements("css selector", ".card-name a")
    
    # Extract the href attribute using purrr's map function
    href <- purrr::map(anchor_tags, ~ .$getElementAttribute("href"))
    df <- tibble(url = unlist(href))
    
    # Append the results to the main data frame
    obgyn_hrefs <- bind_rows(obgyn_hrefs, df)
    
    
    print("One-paged result job completed!")
  } else {
    while (TRUE) {
      # Select all anchor tags within a specific element (e.g., class="card-name")
      anchor_tags <- remDr$findElements("css selector", ".card-name a")
      
      # Extract the href attribute using purrr's map function
      href <- purrr::map(anchor_tags, ~ .$getElementAttribute("href"))
      df <- tibble(url = unlist(href))
      
      # Append the results to the main data frame
      obgyn_hrefs <- bind_rows(obgyn_hrefs, df)
      
      # Find the li HTML element by its CSS selector
      li_pagination_next <- remDr$findElement(using = "css selector", value = "[data-qa-target='pagination--next-page']")  
      
      # Function to check if it's the last page
      is_last_page <- function() {
        last_page_element <- tryCatch({
          remDr$findElement(using = "css", value = paste('a[data-qa-target="pagination--page-',last_page, ' current-page last-page"][aria-current="page"]',sep = ""))
        }, error = function(e) {
          return(NULL)  # If the element is not found, return NULL
        })
        
        # Checks if the last page element exists
        if (!is.null(last_page_element)) {
          return(TRUE)
        } else {
          return(FALSE)
        }
      }
      
      # Check if the preferred class name is present
      if (is_last_page()) {
        break
      } else {
        # Click on the link to potentially load new content
        next_button <- remDr$findElement(using = "css selector", value = "[data-qa-target='pagination--next-page']")
        next_button$clickElement()
        # Wait for some time to allow the new content to load
        Sys.sleep(5)  # You can adjust the sleep time as needed
      }
    }
  }
  
  return(obgyn_hrefs)
}


# Step 2 - scrape all obgyn details using the urls collected from step (1)

scrape_obgyn_details <- function(obgyn_url){
  
  remDr$navigate(obgyn_url)
  
  # Extract text from the HTML element
  # 1a. <h1 data-qa-target="ProviderDisplayName">Dr. Brent Parnell, MD</h1>
  # 1b. <h1 data-qa-target="ProviderDisplayName">Dr. Matthew Wheatley, MD</h1>
  provider_display_name_element <- remDr$findElement(using = "xpath", value = "//h1[@data-qa-target='ProviderDisplayName']")
  provider_display_name_text <- provider_display_name_element$getElementText()[[1]]
  provider_display_name_text
  
  # 2a. <span data-qa-target="ProviderDisplaySpeciality">Female Pelvic Medicine &amp; Reconstructive Surgery<span class="specialty-asterisk">*</span></span>
  # 2b. <span data-qa-target="ProviderDisplaySpeciality">Radiation Oncology<span class="specialty-asterisk">*</span></span>
  provider_display_speciality_element <- remDr$findElement(using = "xpath", value = "//span[@data-qa-target='ProviderDisplaySpeciality']")
  provider_display_speciality_text <- provider_display_speciality_element$getElementText()[[1]]
  provider_display_speciality_text
  
  # 3a. <span data-qa-target="ProviderDisplayGender">Male</span>
  # 3b. <span data-qa-target="ProviderDisplayGender">Male</span>
  provider_display_gender_element <- remDr$findElement(using = "xpath", value = "//span[@data-qa-target='ProviderDisplayGender']")
  provider_display_gender_text <- provider_display_gender_element$getElementText()[[1]]
  provider_display_gender_text
  
  # 4. <span data-qa-target="ProviderDisplayAge">Age 46</span>
  # provider_display_age_element <- remDr$findElement(using = "xpath", value = "//span[@data-qa-target='ProviderDisplayAge']")
  # provider_display_age_text <- provider_display_age_element$getElementText()[[1]]
  # provider_display_age_text <- str_extract(provider_display_age_text,"\\d+")
  # provider_display_age_text
  
  has_age <- function() {
    age_element <- tryCatch({
      remDr$findElement(using = "xpath", value = "//span[@data-qa-target='ProviderDisplayAge']")
    }, error = function(e) {
      return(NULL)  # If the element is not found, return NULL
    })
    
    # Check if the last page element exists
    if (!is.null(age_element)) {
      provider_display_age_element <- remDr$findElement(using = "xpath", value = "//span[@data-qa-target='ProviderDisplayAge']")
      provider_display_age_text <- provider_display_age_element$getElementText()[[1]]
      provider_display_age_text <- str_extract(provider_display_age_text,"\\d+")
      provider_display_age_text
      return(provider_display_age_text)
    } else {
      return(0)
    }
  }
  
  provider_age <- has_age()
  provider_age
  
  # 5. <h3 data-qa-target="Practice-Name">Practice</h3>
  provider_practice_name_element <- remDr$findElement(using = "xpath", value = "//h3[@data-qa-target='Practice-Name']")
  provider_practice_name_text <- provider_practice_name_element$getElementText()[[1]]
  provider_practice_name_text
  
  
  # <span class="visit-practice-link hg-track" data-hgoname="visit-practice-link" data-qa-target="qa-practice-link">The Kirklin Clinic</span>
  # <a class="visit-practice-link hg-track" data-hgoname="visit-practice-link" data-qa-target="qa-practice-link" href="/group-directory/ca-california/carmichael/mercy-medical-group-xndpfl" role="button" tabindex="0">Mercy Cancer Center - Coyle- Radiation Oncology</a>
  # visit_practice_links_elements <- remDr$findElements(using = "xpath", value = "//span[@data-qa-target='qa-practice-link']")
  visit_practice_links_elements <- remDr$findElements(using = "xpath", value = "//*[contains(@data-qa-target,'qa-practice-link')]")
  
  visit_practice_links <- lapply(visit_practice_links_elements, function (x) x$getElementText()) %>% 
    unlist()
  visit_practice_links
  length(visit_practice_links)
  class(visit_practice_links)
  
  # <span class="street-address" data-qa-target="practice-address-street">2000 6th Ave S </span>
  practice_address_street_elements <- remDr$findElements(using = "xpath", value = "//span[@data-qa-target='practice-address-street']")
  
  practice_address_streets <- lapply(practice_address_street_elements, function (x) x$getElementText()) %>% 
    unlist()
  practice_address_streets
  length(practice_address_streets)
  
  # practice_address_city
  # <span data-qa-target="practice-address-city">Birmingham, </span>
  
  practice_address_city_elements <- remDr$findElements(using = "xpath", value = "//span[@data-qa-target='practice-address-city']")
  
  practice_address_cities <- lapply(practice_address_city_elements, function (x) x$getElementText()) %>% 
    unlist()
  practice_address_cities
  
  # <span data-qa-target="practice-address-state">AL </span>
  
  practice_address_state_elements <- remDr$findElements(using = "xpath", value = "//span[@data-qa-target='practice-address-state']")
  
  practice_address_states <- lapply(practice_address_state_elements, function (x) x$getElementText()) %>% 
    unlist()
  practice_address_states
  
  
  # <span data-qa-target="practice-address-postalCode">35233</span>
  
  practice_address_postalCode_elements <- remDr$findElements(using = "xpath", value = "//span[@data-qa-target='practice-address-postalCode']")
  
  practice_address_postalCodes <- lapply(practice_address_postalCode_elements, function (x) x$getElementText()) %>% 
    unlist()
  practice_address_postalCodes
  
  # new-number-visit-section
  # <a class="detail-link phone-number" data-hgoname="visit-phone-number" data-qa-target="new-number-visit-section" href="tel:2059963130" tabindex="-1" title="Call: (205) 996-3130">(205) 996-3130</a>
  # <div class="label-container" data-qa-target="new-number">(916) 306-7951</div>
  new_number_visit_section_elements <- remDr$findElements(using = "xpath", value = "//*[contains(@data-qa-target,'new-number')]")
  
  new_number_visit_sections <- lapply(new_number_visit_section_elements, function (x) x$getElementText()) %>% 
    unlist()
  new_number_visit_sections
  
  
  scrape_obgyn_details <- data.frame(
    obgyn_id = str_extract(obgyn_url,pattern = "(?<=\\/)([a-z0-9]+[-])+[a-z0-9]+"),
    provider_display_name=provider_display_name_text,
    provider_display_speciality=provider_display_speciality_text,
    provider_display_gender = provider_display_gender_text,
    provider_display_age= provider_age,
    provider_practice_name = provider_practice_name_text,
    visit_practice_name = visit_practice_links,
    practice_address_street = practice_address_streets,
    practice_address_city = practice_address_cities,
    practice_address_state = practice_address_states,
    practice_address_postalCode = practice_address_postalCodes,
    phone_number = new_number_visit_sections
  )
  
  return(scrape_obgyn_details)
  
}




