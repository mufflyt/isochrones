# Setup
source("Code/01-setup.R")



# Linear regression

# Equation of linear regression: Sum of Allowable Medicare Charges~.
rmarkdown::render("Code/~linear_regression/equation.Rmd")

# Build the linear regression model for Sum of Allowable Medicare Charges
source("Code/~linear_regression/Payment regression Rcode 2.R")
