
install.packages('reticulate')
library(reticulate)

switch(
  Sys.info()[["user"]],
  "rstudio-connect" = source("/home/data-science/data_science/util/util_functions.R"),
  source("~/data_science/util/util_functions.R")
)

# use_python("/usr/local/bin/python")

py_run_file("script.py")

