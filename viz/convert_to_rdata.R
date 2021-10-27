library(data.table)
require(dplyr)
library(plotly)

#setwd("~/git/nft-deal-score/viz")

pred_price <- read.csv('~/nft-deal-score/data/pred_price.csv') %>% as.data.table()
pred_price[, token_id := as.numeric(token_id) ]
pred_price <- pred_price[order(token_id)]

attributes <- read.csv('~/nft-deal-score/data/attributes.csv') %>% as.data.table()
feature_values <- read.csv('~/nft-deal-score/data/feature_values.csv') %>% as.data.table()
sales <- read.csv('~/nft-deal-score/data/sales.csv',colClasses = c("character")) %>% as.data.table()

save(pred_price, attributes, feature_values,sales,
     file='~/nft-deal-score/viz/data.Rdata')
