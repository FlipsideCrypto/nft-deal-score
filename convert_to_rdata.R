library(data.table)
library(dplyr)
library(plotly)

isRstudio <- Sys.info()[["user"]] == 'rstudio-connect'

file.location <- ifelse(
	isRstudio
	, "/rstudio-data/"
	, '~/git/nft-deal-score/viz/'
)

read_csv <- function(fname) {
	dir <- ifelse(isRstudio, '/rstudio-data/', '~/git/nft-deal-score/data/')
	fname <- paste0(dir, fname)
	dt <- read.csv(fname) %>% as.data.table()
}

# load all csvs
pred_price <- read_csv('pred_price.csv')
pred_price[, token_id := as.numeric(token_id) ]
pred_price <- pred_price[ collection != 'meerkatmillionaires' ]
pred_price <- pred_price[order(token_id)]

attributes <- read_csv('attributes.csv')
attributes[, feature_name := trimws(feature_name) ]
attributes[, feature_value := trimws(as.character(feature_value)) ]
feature_values <- read_csv('feature_values.csv')
sales <- read_csv('model_sales.csv')
listings <- read.csv('/Users/kellenblumberg/git/nft-deal-score/viz/nft_deal_score_listings.csv') %>% as.data.table()
coefsdf <- read_csv('coefsdf.csv')
tokens <- read_csv('tokens.csv')
tokens[, token_id := clean_token_id]
sales[, price := as.numeric(price)]
sales[, token_id := as.numeric(token_id)]
listings[, token_id := as.numeric(token_id)]
listings <- listings[ !(collection == 'Stoned Ape Crew' & token_id == 764) ]
listings <- listings[ !(collection == 'Solana Monkey Business' & token_id == 953) ]
tokens[, token_id := as.numeric(token_id)]

# manual adjustments to price
# ids_1 <- attributes[ (collection == 'Aurory') & (feature_value == 'Solana Blob') ]$token_id
# pred_price[  collection == 'Aurory' & token_id %in% eval(ids_1), pred_price := (pred_price * 0.8) ]

# ids_2 <- attributes[ (collection == 'Aurory') & (feature_value == 'Long Blob Hair ') ]$token_id
# pred_price[  collection == 'Aurory' & token_id %in% eval(ids_2), pred_price := (pred_price * 0.90) ]

# ids_3 <- attributes[ (collection == 'Aurory') & (grepl( 'Mask', feature_value, fixed = TRUE)) ]$token_id
# pred_price[  collection == 'Aurory' & token_id %in% eval(ids_3), pred_price := (pred_price * 0.975) ]

# sales[collection == 'Cets On Creck', collection := 'Cets on Creck']
# pred_price[collection == 'Cets On Creck', collection := 'Cets on Creck']
listings[collection == 'Cets On Creck', collection := 'Cets on Creck']
cols <- c( 'Citizens By Solsteads' )
# sales[, tmp := tolower(coll)]
for (col in cols) {
	sales[ tolower(collection) == eval(tolower(col)), collection := eval(col) ]
	pred_price[ tolower(collection) == eval(tolower(col)), collection := eval(col) ]
	listings[ tolower(collection) == eval(tolower(col)), collection := eval(col) ]
}


sort(unique(listings$collection))
sort(unique(pred_price$collection))
sort(unique(sales$collection))

# filter for only collections that have all data
a <- unique(pred_price[, list(collection)][order(collection)])
b <- unique(sales[, list(collection)][order(collection)])
c <- unique(listings[, list(collection)][order(collection)])
d <- merge(merge(a, b), c)
d <- d[order(collection)]
d <- d[ collection %in% c('Aurory','Bubblegoose Ballers','Catalina Whale Mixer','Cets on Creck','DeGods','Degen Apes','Famous Fox Federation','Meerkat Millionaires','Okay Bears','Pesky Penguins','Primates','SOLGods','Solana Monkey Business','Stoned Ape Crew','ThugbirdzcMAYC') ]
write.csv(d, '~/Downloads/tmp.csv', row.names=F)

pred_price <- merge(pred_price, d, by=c('collection'))
attributes <- merge(attributes, d, by=c('collection'))
feature_values <- merge(feature_values, d, by=c('collection'))
sales <- merge(sales, d, by=c('collection'))
listings <- merge(listings, d, by=c('collection'))
coefsdf <- merge(coefsdf, d, by=c('collection'))
tokens <- merge(tokens, d, by=c('collection'))


save(
	pred_price
	, attributes
	, feature_values
	, sales
	, listings
	, coefsdf
	, tokens
	, file = paste0(file.location,'data.Rdata')
)
save(
	pred_price
	, attributes
	, feature_values
	, sales
	, coefsdf
	, tokens
	, file = paste0(file.location,'nft_deal_score_data.Rdata')
)

# save(
# 	listings
# 	, file = paste0(file.location,'nft_deal_score_listings_data.Rdata')
# )
