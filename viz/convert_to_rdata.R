library(data.table)
require(dplyr)
library(plotly)

setwd("~/git/nft-deal-score/viz")

pred_price <- read.csv('../data/pred_price.csv') %>% as.data.table()
pred_price[, token_id := as.numeric(token_id) ]
pred_price <- pred_price[order(token_id)]
unique(pred_price$collection)

pred_price %>% group_by(collection) %>% summarize(pred_price=min(pred_price))

attributes <- read.csv('../data/attributes.csv') %>% as.data.table()
feature_values <- read.csv('../data/feature_values.csv') %>% as.data.table()
sales <- read.csv('../data/model_sales.csv') %>% as.data.table()
listings <- read.csv('../data/listings.csv') %>% as.data.table()
coefsdf <- read.csv('../data/coefsdf.csv') %>% as.data.table()
tokens <- read.csv('../data/tokens.csv') %>% as.data.table()
# sales$collection <- 'Solana Monkey Business'

pred_price <- pred_price[ collection != 'meerkatmillionaires' ]
unique(pred_price$collection)

# pred_price <- head(pred_price)
# attributes <- head(attributes)
# feature_values <- head(feature_values)
# sales <- head(sales)

save(pred_price, attributes, feature_values, sales, listings, coefsdf, tokens, file='data.Rdata')
