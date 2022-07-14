---
title: "Update NFT Deal Score Data"
author: "Kellen"
date: "2022-04-20"
output: html_document
---

```{r setup, include=FALSE}
knitr::opts_chunk$set(echo = TRUE)
```

## Run Updates

Hello I am running this at `r Sys.time()`

```{r update}
#include all required libraries here
#EVEN IF YOU SOURCE util_functions.R 
#YOU HAVE TO PUT THE LIBRARIES HERE I KNOW SORRY
#BUT HERE THEY ALL ARE TO SAVE YOU TIME
# install.packages('RCurl')
library(RCurl)
library(fasttime)
library(gridExtra)
library(ggplot2)
library(data.table)
library(reshape2)
library(dplyr)
library(dbplyr)
library(RJSONIO)
library(magrittr)
library(RJSONIO)
library(xts)
library(quantmod)
library(fTrading)
library(curl)
library(stringr)
library(aws.s3)
library(RPostgres)
library(odbc)
library(httr)
library(jsonlite)
library(reticulate)

#NOW COPY EVERYTHING ELSE FROM YOUR CURRENT
#update_data.R FILE HERE ---------->
# virtualenv_create('pyvenv')
# use_virtualenv('pyvenv')
# virtualenv_install('pyvenv', 'pandas')
# virtualenv_install('pyvenv', 'pandas')
# py_install('cloudscraper', pip = TRUE)
# r reticulate python ModuleNotFoundError
# print('54')

SD_MULT = 3
SD_SCALE = 1.95

user <- Sys.info()[['user']]
# isRstudio <- user == 'rstudio-connect'
isRstudio <- user != 'kellenblumberg'
# isRstudio <- TRUE
if (isRstudio) {
	use_python('/opt/python/3.10.4/bin/python')
	py_install('pandas', pip = TRUE)
	py_install('snowflake-connector-python', pip = TRUE)
}

base_dir <- ifelse(
	isRstudio
	, '/rstudio-data/'
	, ifelse(user == 'fcaster'
		, '/srv/shiny-server/nft-deal-score/'
		, '~/git/nft-deal-score/viz/'
	)
)

if(isRstudio) {
	source('/home/data-science/data_science/util/util_functions.R')
	source('/home/data-science/data_science/util/kafka_utils.R')
	source_python('/home/data-science/data_science/viz/nft-deal-score/add_sales.py')
	source_python('~/upload_solana_nft_labels.py')
} else {
	source('~/data_science/util/util_functions.R')
	source('~/data_science/util/kafka_utils.R')
	source_python(paste0(base_dir, 'scrape_terra_nfts.py'))
	source_python(paste0(base_dir, 'add_sales.py'))
}

usr <- readLines(file.path(base.path,"data_science/util/snowflake.usr"))
pwd <- readLines(file.path(base.path,"data_science/util/snowflake.pwd"))

load(paste0(base_dir,'nft_deal_score_data.RData'))

listings_file <- paste0(base_dir,'nft_deal_score_listings_data.RData')
sales_file <- paste0(base_dir,'nft_deal_score_sales_data.RData')
load(listings_file)


coefsdf[, tot := lin_coef + log_coef ]
coefsdf[, lin_coef := lin_coef / tot]
coefsdf[, log_coef := log_coef / tot]
sum(coefsdf$log_coef) + sum(coefsdf$lin_coef)

# write sales data to nft_deal_score_sales.csv
add_solana_sales(usr, pwd, base_dir)
add_ethereum_sales(usr, pwd, base_dir)
# add_terra_sales(usr, pwd, base_dir)

# read sales data from nft_deal_score_sales.csv
raw_sales <- read.csv(paste0(base_dir,'nft_deal_score_sales.csv')) %>% as.data.table()
raw_sales <- raw_sales[order(collection, sale_date, price)]
unique(raw_sales$collection)

# calculate the floor price
raw_sales <- raw_sales %>%
	group_by(collection) %>%
	mutate(mn_20=lag(price, 1)) %>% 
	as.data.table()

raw_sales <- raw_sales %>%
	group_by(collection) %>%
	mutate(rolling_floor=rollapply(mn_20, width = 20, FUN = "quantile", p = .0575, na.pad = TRUE, align = 'right')) %>% 
	as.data.table()

raw_sales[, rolling_floor := nafill(rolling_floor, type = "nocb")]


# calculate the fair market price
tmp <- merge( raw_sales[, list(collection, token_id, sale_date, price, tx_id, rolling_floor)], coefsdf, by=c('collection') )
tmp <- merge( tmp, pred_price, by=c('collection','token_id') )
tmp[, abs_chg := (rolling_floor - floor_price) * lin_coef ]
tmp[, pct_chg := (rolling_floor - floor_price) * log_coef ]
tmp[, fair_market_price := pred_price + abs_chg + (pct_chg * pred_price / floor_price) ]

# save to an .RData file
sales <- tmp[, list(collection, token_id, sale_date, price, nft_rank, fair_market_price, rolling_floor)]
colnames(sales) <- c('collection', 'token_id', 'block_timestamp', 'price', 'nft_rank', 'pred', 'mn_20')
save(
	sales
	, file = sales_file
)


# load the mints
query <- '
	SELECT DISTINCT project_name AS collection
	, mint AS tokenMint
	, token_id
	FROM solana.dim_nft_metadata
'
mints <- QuerySnowflake(query)
colnames(mints) <- c('collection','tokenMint','token_id')
# mints[ collection == 'Cets On Creck', collection := 'Cets on Creck']

# pull terra listings
# terra_listings <- scrape_randomearth(base_dir)
# head(terra_listings)
# unique(terra_listings$collection)


# 9c39e05c-db3c-4f3f-ac48-84099111b813
get_me_url <- function(collection, offset) {
	return(paste0('https://api-mainnet.magiceden.dev/v2/collections/',collection,'/listings?offset=',offset,'&limit=20'))
}
get_smb_url <- function(page) {
	return(paste0('https://market.solanamonkey.business/api/items?limit=40&page=',page))
}

solana_listings <- data.table()

solana_collections <- c(
	'famous_fox_federation'
)
solana_collections <- c(
	'blocksmith_labs'
	, 'dazedducks_metagalactic_club'
	, 'degenerate_trash_pandas'
	, 'famous_fox_federation'
	, 'generous_robots_dao'
	, 'ghostface'
	, 'ghostface'
	, 'ghostface_gen_2'
	, 'portals'
	, 'smokeheads'
	, 'theorcs'
)

solana_collections <- c(

	# 'blocksmith_labs'
	# , 'dazedducks_metagalactic_club'
	# , 'degenerate_trash_pandas'
	'famous_fox_federation',
	# , 'generous_robots_dao'
	# , 'ghostface'
	# , 'ghostface_gen_2'
	# , 'portals'
	# , 'smokeheads'
	# , 'theorcs',
	# 'astrals',
	'aurory',
	# 'bohemia_',
	# 'bothead',
	'bubblegoose_ballers',
	# 'cat_cartel',
	'cets_on_creck',
	# 'citizens_by_solsteads',
	# 'communi3',
	# 'defi_pirates',
	# 'degendojonft',
	'degenerate_ape_academy',
	# 'degenerate_ape_kindergarten',
	'degods',
	# 'doge_capital',
	# 'galactic_gecko_space_garage',
	# 'justape',
	# 'looties',
	# 'marinadechefs',
	'meerkat_millionaires_country_club',
	# 'monkey_baby_business',
	'okay_bears',
	'pesky_penguins',
	'portals',
	'primates',
	# 'psykerhideouts',
	# 'quantum_traders',
	# 'solana_monke_rejects',
	'solana_monkey_business',
	# 'solanauts',
	'solgods',
	# 'solstein',
	'stoned_ape_crew',
	# 'taiyo_infants_incubators',
	'the_catalina_whale_mixer',
	# 'the_remnants_',
	# 'the_tower',
	# 'the_vaultx_dao',
	'thugbirdz'
	# 'trippin_ape_tribe',
	# 'visionary_studios'
)
# headers = c(
# 	'Authorization': 'Bearer 9c39e05c-db3c-4f3f-ac48-84099111b813'
# )
for(collection in solana_collections) {
	print(paste0('Working on ', collection, '...'))
	has_more <- TRUE
	has_err <- FALSE
	offset <- 0
	while(has_more) {
		Sys.sleep(1)
		out <- tryCatch(
			{
				print(paste0('Offset #', offset))
				url <- get_me_url(collection, offset)
				response <- GET(
					url = url
					# , add_headers(.headers = c('Authorization'= 'Bearer 9c39e05c-db3c-4f3f-ac48-84099111b813'))
					, add_headers('Authorization'= 'Bearer 9c39e05c-db3c-4f3f-ac48-84099111b813')
				)
				# r <- content(response, as = 'parsed')
				content <- rawToChar(response$content)
				content <- fromJSON(content)
				if( !is.data.frame(content) ) {
					content <- rbindlist(content, fill=T) 
				}
				has_more <- nrow(content) > 0
				if(nrow(content) > 0 && length(content) > 0) {
					# content <- data.table(content)
					df <- merge(content, mints, by=c('tokenMint')) %>% as.data.table()
					# if(nrow(df) > 0) {
					# 	print(min(df$price))
					# }
					df <- df[, list(collection, token_id, price)]
					solana_listings <- rbind(solana_listings, df)
				} else {
					has_more <- FALSE
				}
				offset <- offset + 20
				has_err <- FALSE
			},
			error=function(cond) {
				print(paste0('Error: ', cond))
				return(TRUE)
				# has_more <- FALSE
				# if(has_err) {
				# 	has_err <- FALSE
				# 	has_more <- FALSE
				# 	return(TRUE)
				# } else {
				# 	Sys.sleep(15)
				# 	has_err <- TRUE
				# 	return(FALSE)
				# }
				# return(TRUE)
			},
			warning=function(cond) {
				print(paste0('Warning: ', cond))
				return(TRUE)
			},
			finally={
				# return(TRUE)
				# print(paste0('Finally'))
			}
		)
		if(out) {
			offset <- offset + 20
			# has_more <- FALSE
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
# head(terra_listings)
# new_listings <- rbind(solana_listings, terra_listings)
new_listings <- unique(solana_listings)

# listings <- read.csv('./data/listings.csv') %>% as.data.table()
rem <- unique(new_listings$collection)
sort(rem)
listings <- listings[ !(collection %in% eval(rem)), ]
listings <- listings[, list(collection, token_id, price)]
listings <- rbind(listings, new_listings)
listings <- listings[order(collection, price)]
listings[, token_id := as.integer(token_id)]
listings <- listings[ !(collection %in% c('LunaBulls','Galactic Punks','Galactic Angels','Levana Dragon Eggs')) ]

listings <- listings[!is.na(price)]
listings <- listings %>% as.data.table()

sort(unique(listings$collection))
# write.csv(unique(listings[, collection]), '~/Downloads/tmp.csv', row.names=F)

floors <- listings %>% 
	group_by(collection) %>%
	summarize(cur_floor = min(price)) %>%
	as.data.table()


get_fmp <- function(data, coefsdf, pred_price) {
	coefsdf[, tot := lin_coef + log_coef ]
	coefsdf[, lin_coef := lin_coef / tot]
	coefsdf[, log_coef := log_coef / tot]
	sum(coefsdf$log_coef) + sum(coefsdf$lin_coef)

	fmp <- merge( pred_price, coefsdf, by=c('collection') )
	fmp <- merge( fmp, data[, list(token_id, collection, block_timestamp, price, mn_20)], by=c('token_id','collection') )
	# fmp <- merge( fmp, floors, by=c('collection') )
	fmp[, abs_chg := (mn_20 - floor_price) * lin_coef ]
	fmp[, pct_chg := (mn_20 - floor_price) * log_coef ]
	fmp[, fair_market_price := pred_price + abs_chg + (pct_chg * pred_price / floor_price) ]
}

if(FALSE) {
	coefsdf[, tot := lin_coef + log_coef ]
	coefsdf[, lin_coef := lin_coef / tot]
	coefsdf[, log_coef := log_coef / tot]
	sum(coefsdf$log_coef) + sum(coefsdf$lin_coef)

	fmp <- merge( pred_price, coefsdf, by=c('collection') )
	fmp <- merge( fmp, floors, by=c('collection') )
	fmp[, abs_chg := (cur_floor - floor_price) * lin_coef ]
	fmp[, pct_chg := (cur_floor - floor_price) * log_coef ]
	fmp[, fair_market_price := pred_price + abs_chg + (pct_chg * pred_price / floor_price) ]

	mn <- fmp %>% group_by(collection, cur_floor) %>% summarize(mn = min(fair_market_price)) %>% as.data.table()
	mn[, ratio := cur_floor / mn]
	fmp <- merge(fmp, mn[, list(collection, ratio)])
	fmp[ratio < 1, fair_market_price := fair_market_price * ratio ]

	fmp[, cur_sd := pred_sd * (cur_floor / floor_price) * SD_SCALE ]
	fmp[, price_low := qnorm(.2, fair_market_price, cur_sd) ]
	fmp[, price_high := qnorm(.8, fair_market_price, cur_sd) ]

	fmp[, price_low := pmax(price_low, cur_floor * 0.975) ]
	fmp[, price_high := pmax(price_high, cur_floor * 1.025) ]

	fmp[, price_low := round(price_low, 2) ]
	fmp[, price_high := round(price_high, 2) ]
	fmp[, fair_market_price := pmax(cur_floor, fair_market_price) ]
	fmp[, fair_market_price := round(fair_market_price, 2) ]
	fmp[, cur_sd := round(cur_sd, 2) ]
	head(fmp[collection == 'SOLGods'][order(fair_market_price)])
	head(fmp[(collection == 'SOLGods') & (rk <= 4654)][order(fair_market_price)])
	head(fmp[(collection == 'SOLGods') & (rk == 4654)][order(fair_market_price)])

	tmp <- merge(listings, fmp, by = c('collection','token_id')) %>% as.data.table()
	tmp[, deal_score := pnorm(price, fair_market_price, cur_sd) ]
	tmp[, deal_score := 100 * (1 - deal_score) ]
	tmp[, vs_floor := (price / cur_floor) - 1 ]
	tmp[, vs_floor_grp := ifelse(vs_floor < .1, '<10%', ifelse(vs_floor < .25, '<25%', '>25%')) ]
	tmp[, vs_floor := (price - cur_floor) ]
	tmp <- tmp[ !(collection %in% c('Levana Dragon Eggs','Galactic Punks','LunaBulls','Galactic Angels','MAYC')) ]


	t2 <- tmp[order(-deal_score),.SD[2], list(vs_floor_grp, collection)] %>% as.data.table()
	t2 <- t2[, list(collection, vs_floor_grp, deal_score)][order(collection, vs_floor_grp)]
	t3 <- tmp[order(-deal_score),.SD[3], list(vs_floor_grp, collection)] %>% as.data.table()
	t3 <- t3[, list(collection, vs_floor_grp, deal_score)][order(collection, vs_floor_grp)]
	colnames(t2) <- c('collection','vs_floor_grp','deal_score_g2')
	colnames(t3) <- c('collection','vs_floor_grp','deal_score_g3')
	tmp <- merge(tmp, t2, by=c('collection','vs_floor_grp'))
	tmp <- merge(tmp, t3, by=c('collection','vs_floor_grp'))


	t2 <- tmp[order(-deal_score),.SD[2], list(collection)] %>% as.data.table()
	t2 <- t2[, list(collection, deal_score)][order(collection)]
	t3 <- tmp[order(-deal_score),.SD[3], list(collection)] %>% as.data.table()
	t3 <- t3[, list(collection, deal_score)][order(collection)]
	colnames(t2) <- c('collection','deal_score_2')
	colnames(t3) <- c('collection','deal_score_3')

	tmp <- merge(tmp, t2, by=c('collection'))
	tmp <- merge(tmp, t3, by=c('collection'))

	tmp[, pts := (deal_score * 5 - deal_score_g2 - deal_score_g3 - deal_score_2 - deal_score_3) * ((cur_floor / price)**0.75) + (100 * (1 - (( price - cur_floor ) / (fair_market_price - cur_floor)))) ]
	url <- 'https://discord.com/api/webhooks/976332557996150826/8KZqD0ov5OSj1w4PjjLWJtmgnCM9bPWaCkZUUEDMeC27Z0iqiA-ZU5U__rYU9tQI_ijA'
	unique(tmp$collection)
	for(col in c('price','pred_price','fair_market_price','vs_floor','deal_score','deal_score_2','deal_score_3','pts')) {
		if(!'price' %in% col) {
			tmp[, eval(col) := round(get(col)) ]
		} else {
			tmp[, eval(col) := ifelse(
				get(col) < 10
				, round(get(col), 2)
				, ifelse(
					get(col) < 100
					, round(get(col), 1)
					, round(get(col)))
				)
			]
		}
	}
	tmp <- tmp[order(-pts)]
	head(tmp[, list(collection, token_id, price, nft_rank, rk, pred_price, cur_floor, fair_market_price, deal_score, deal_score_2, deal_score_3, pts)], 20)
	head(tmp[, list(collection, token_id, price, nft_rank, rk, pred_price, cur_floor, fair_market_price, deal_score, deal_score_2, deal_score_3, pts)], 20)
	paste(head(tmp$label), collapse='\n')

	tmp[, l := nchar(collection)]
	mx <- max(tmp$l)
	# tmp$clean_collection <- str_pad(collection, eval(mx) - l, side = 'right', pad = '-') ]
	tmp$n_pad <- mx - tmp$l
	tmp$clean_collection <- str_pad(tmp$collection, mx - tmp$l, side = 'right', pad = '-')
	tmp[, clean_collection := str_pad(collection, eval(mx), pad='-', side='right')]
	tmp[, clean_collection := str_pad(collection, eval(mx), pad='-', side='both')]
	tmp$clean_collection <- str_pad(tmp$collection, mx, pad='-', )
	tmp[, label := paste(clean_collection, str_pad(token_id, 4, side='left'), price, fair_market_price, deal_score, sep='\t')]
	tmp[, label := paste(
		clean_collection
		, str_pad(token_id, 4, side='left')
		, str_pad(rk, 4, side='left')
		, str_pad(price, 4, side='left')
		, str_pad(vs_floor, 5, side='left')
		, str_pad(fair_market_price, 4, side='left')
		, str_pad(deal_score, 2, side='left')
		, str_pad(deal_score_2, 2, side='left')
		, str_pad(deal_score_3, 2, side='left')
		, str_pad(pts, 3, side='left')
		, sep='\t')
	]
	header <- paste(
		str_pad('collection', mx, side='both', pad='-')
		, str_pad('id', 4, side='left')
		, str_pad('rk', 4, side='left')
		, str_pad('$', 3, side='left')
		, str_pad('floor', 5, side='left')
		, str_pad('fmp', 3, side='left')
		, str_pad('ds', 2, side='left')
		, str_pad('ds2', 2, side='left')
		, str_pad('ds3', 2, side='left')
		, str_pad('pts', 3, side='left')
		, sep='\t')

	tmp <- tmp[order(-pts)]
	content <- tmp[ (price < 0.9 * fair_market_price) , head(.SD, 2), by = collection]
	content <- head(content[order(-pts)], 15)
	# content <- paste(c(header, content$label, collapse='\n'))

	content <- paste(c(header, content$label), collapse='\n')
	# content <- paste(c(header, head(tmp$label, 10)), collapse='\n')
	data <- list(
		content = paste0('```',content,'```')
	)
	res <- POST(url, body = data, encode = "form", verbose())

	# tmp <- tmp[order(-deal_score)]
	# head(tmp)
	# plot_data[, deal_score := round(100 * (1 - y))]
	# y <- pnorm(x, mu, sd)
	# tmp[, deal_score := ((fair_market_price / price) - 1) ]
	# tmp[, deal_score := ((fair_market_price / price) - 0) ]
	# tmp <- tmp[order(-deal_score)]
	# tmp <- tmp[, list(collection, token_id, fair_market_price, price, deal_score)]
	# tmp[, .SD[1:3], collection]

	# fmp <- fmp[, list(collection, token_id, nft_rank, rk, fair_market_price, price_low, price_high)]
	fmp <- fmp[, list(collection, token_id, nft_rank, rk, fair_market_price, cur_floor, cur_sd, lin_coef, log_coef)]
	colnames(fmp)[3] <- 'rarity_rank'
	colnames(fmp)[4] <- 'deal_score_rank'

	if (FALSE) {
		for( cur_collection in unique(fmp$collection)) {
			print(paste0('Working on ',cur_collection, '...'))
			data <- fmp[collection == eval(cur_collection)]
			KafkaGeneric(
				.topic = 'prod-data-science-uploads'
				, .url = 'https://kafka-rest-proxy.flipside.systems'
				, .project = paste0('nft-deal-score-rankings-', cur_collection)
				, .data = data
			)
		}
	}
}

# write the floor prices to snowflake
data <- floors
KafkaGeneric(
	.topic = 'prod-data-science-uploads'
	, .url = 'https://kafka-rest-proxy.flipside.systems'
	, .project = 'nft-deal-score-floors'
	, .data = data
)

sort(unique(listings$collection))

save(
	listings
	, file = listings_file
)
if(!isRstudio) {
	write.csv(listings, paste0(base_dir, 'nft_deal_score_listings.csv'))
}

```

Done updating at `r Sys.time()`

The end. Byeeeee. 