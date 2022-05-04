# install.packages('reticulate')
library(reticulate)
library(httr)
library(jsonlite)

user <- Sys.info()[['user']]
isRstudio <- user == 'rstudio-connect'

# nft_deal_score_listings_data.RData
base_dir <- ifelse(
	user == 'rstudio-connect'
	, '/rstudio-data/'
	, ifelse(user == 'fcaster'
		, '/srv/shiny-server/nft-deal-score/'
		, '~/git/nft-deal-score/viz/'
	)
)
# base_dir <- '/srv/shiny-server/nft-deal-score/'
listings_file <- paste0(base_dir,'nft_deal_score_listings_data.RData')
load(listings_file)

if(isRstudio) {
	source('/home/data-science/data_science/util/util_functions.R')
	source_python('/home/data-science/data_science/nft-deal-score/scrape_terra_nfts.py')
} else {
	source('~/data_science/util/util_functions.R')
	source_python(paste0(base_dir, 'scrape_terra_nfts.py'))
}

# py_install('pandas', pip = TRUE)
# py_install('cloudscraper', pip = TRUE)
# py_install('snowflake-connector-python', pip = TRUE)
# cloudscraper <- import('cloudscraper')

base_dir <- ifelse(
	user == 'rstudio-connect'
	, '/rstudio-data/'
	, ifelse(user == 'fcaster'
		, '/srv/shiny-server/nft-deal-score/'
		, '~/git/nft-deal-score/viz/'
	)
)
source_python(paste0(base_dir, 'scrape_terra_nfts.py'))
source_python(paste0(base_dir, 'add_sales.py'))

query <- '
	SELECT DISTINCT project_name AS collection
	, mint AS tokenMint
	, token_id
	FROM solana.dim_nft_metadata
'
mints <- QuerySnowflake(query)
colnames(mints) <- c('collection','tokenMint','token_id')

# pull terra listings
terra_listings <- scrape_randomearth(base_dir)
head(terra_listings)
unique(terra_listings$collection)


get_me_url <- function(collection, offset) {
	return(paste0('https://api-mainnet.magiceden.dev/v2/collections/',collection,'/listings?offset=',offset,'&limit=20'))
}
get_smb_url <- function(page) {
	return(paste0('https://market.solanamonkey.business/api/items?limit=40&page=',page))
}

solana_listings <- data.table()

solana_collections <- c(
	'okay_bears','the_catalina_whale_mixer','meerkat_millionaires_country_club','solgods','cets_on_creck','stoned_ape_crew','degods','aurory','thugbirdz','solana_monkey_business','degenerate_ape_academy','pesky_penguins'
)
for(collection in solana_collections) {
	print(paste0('Working on ', collection, '...'))
	has_more <- TRUE
	offset <- 0
	while(has_more) {
		Sys.sleep(1)
		print(paste0('Offset #', offset))
		url <- get_me_url(collection, offset)
		response <- GET(url)
		content <- rawToChar(response$content)
		content <- fromJSON(content)
		if( typeof(content) == 'list' ) {
			content <- rbindlist(content, fill=T) 
		}
		has_more <- nrow(content) >= 20
		if(nrow(content) > 0 && length(content) > 0) {
			df <- merge(content, mints, by=c('tokenMint')) %>% as.data.table()
			df <- df[, list(collection, token_id, price)]
			offset <- offset + 20
			solana_listings <- rbind(solana_listings, df)
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
		Sys.sleep(1)
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
			solana_listings <- rbind(solana_listings, df)
		}
	}
}

head(solana_listings)
head(terra_listings)
new_listings <- rbind(solana_listings, terra_listings)
new_listings <- unique(new_listings)

# listings <- read.csv('./data/listings.csv') %>% as.data.table()
rem <- unique(new_listings$collection)
rem
listings <- listings[ !(collection %in% eval(rem)), ]
listings <- listings[, list(collection, token_id, price)]
listings <- rbind(listings, new_listings)
listings <- listings[order(collection, price)]
listings[, token_id := as.integer(token_id)]

save(
	listings
	, file = listings_file
)

