
isRstudio <- Sys.info()[["user"]] == 'rstudio-connect'
if(isRstudio) {
	source("/home/data-science/data_science/util/util_functions.R")
} else {
	source('~/git/nft-deal-score/scrape_listings.R')
}
source("/home/data-science/data_science/util/util_functions.R")