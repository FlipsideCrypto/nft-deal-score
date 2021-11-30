library(data.table)
library(dplyr)
library(plotly)

pred_price <- read.csv('~/nft-deal-score/data/pred_price.csv') %>% as.data.table()
pred_price[, token_id := as.numeric(token_id) ]
pred_price <- pred_price[ collection != 'meerkatmillionaires' ]
pred_price <- pred_price[order(token_id)]

attributes <- read.csv('~/nft-deal-score/data/attributes.csv') %>% as.data.table()
feature_values <- read.csv('~/nft-deal-score/data/feature_values.csv') %>% as.data.table()
sales <- read.csv('~/nft-deal-score/data/model_sales.csv') %>% as.data.table()
listings <- read.csv('~/nft-deal-score/data/listings.csv') %>% as.data.table()
coefsdf <- read.csv('~/nft-deal-score/data/coefsdf.csv') %>% as.data.table()
tokens <- read.csv('~/nft-deal-score/data/tokens.csv') %>% as.data.table()


# save(pred_price, attributes, feature_values, sales, listings, coefsdf, tokens, file='data.Rdata')
save(pred_price, attributes, feature_values, sales, listings, coefsdf, tokens, file='~/nft-deal-score/viz/data.Rdata')
