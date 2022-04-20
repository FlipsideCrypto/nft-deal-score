
isRstudio <- Sys.info()[["user"]] == 'rstudio-connect'
if(isRstudio) {
	source("/home/data-science/data_science/util/util_functions.R")
} else {
	source("~/data_science/util/util_functions.R")
	setwd('~/git/nft-deal-score')
}

library(httr)
library(jsonlite)


query <- "
	SELECT DISTINCT project_name AS collection
	, mint AS tokenMint
	, token_id
	FROM solana.dim_nft_metadata
"
mints <- QuerySnowflake(query)
colnames(mints) <- c('collection','tokenMint','token_id')

collections <- c(
	'meerkat_millionaires_country_club','solgods','cets_on_creck','stoned_ape_crew','degods','aurory','thugbirdz','solana_monkey_business','degenerate_ape_academy','pesky_penguins'
)

get_me_url <- function(collection, offset) {
	return(paste0('https://api-mainnet.magiceden.dev/v2/collections/',collection,'/listings?offset=',offset,'&limit=20'))
}
get_smb_url <- function(page) {
	return(paste0('https://market.solanamonkey.business/api/items?limit=40&page=',page))
}

new_listings <- data.table()
collection <- collections[1]
for(collection in collections) {
	print(paste0('Working on ', collection, '...'))
	has_more <- TRUE
	offset <- 0
	while(has_more) {
		print(paste0('Offset #', offset))
		url <- get_me_url(collection, offset)
		response <- GET(url)
		content <- rawToChar(response$content)
		content <- fromJSON(content)
		# content <- rbindlist(content, fill=T) 
		has_more <- nrow(content) >= 20
		if(nrow(content) > 0 && length(content) > 0) {
			df <- merge(content, mints, by=c('tokenMint')) %>% as.data.table()
			df <- df[, list(collection, token_id, price)]
			offset <- offset + 20
			new_listings <- rbind(new_listings, df)
		} else {
			has_more <- FALSE
		}
	}
}

for(collection in c('Solana Monkey Business')) {
	print(paste0('Working on ', collection, '...'))
	has_more <- TRUE
	page <- 1
	while(has_more) {
		print(paste0('Page #', page))
		url <- get_smb_url(page)
		response <- GET(url)
		content <- rawToChar(response$content)
		content <- fromJSON(content)
		# content <- rbindlist(content, fill=T)
		content <- content %>% as.data.table()
		has_more <- nrow(content) > 0 && 'price' %in% colnames(content)
		if(has_more) {
			content <- content[, list(mint, price)]
			content <- unique(content)
			content$price <- as.numeric(content$price) / (10^9)
			has_more <- nrow(content) >= 40
			colnames(content)[1] <- 'tokenMint'
			df <- merge(content, mints, by=c('tokenMint')) %>% as.data.table()
			df <- df[, list(collection, token_id, price)]
			page <- page + 1
			new_listings <- rbind(new_listings, df)
		}
	}
}
new_listings <- unique(new_listings)

listings <- read.csv('./data/listings.csv') %>% as.data.table()
rem <- unique(new_listings$collection)
listings <- listings[ !(collection %in% eval(rem)), ]
listings <- listings[, list(collection, token_id, price)]
listings <- rbind(listings, new_listings)
listings <- listings[order(collection, price)]

write.csv(listings, './data/listings.csv', row.names=F)

