library(httr)
library(RCurl)
library(jsonlite)
library(stringr)

switch(
	Sys.info()[["user"]],
	"rstudio-connect" = source("/home/data-science/data_science/util/util_functions.R"),
	source("~/data_science/util/util_functions.R")
)

switch(
  Sys.info()[["user"]],
  "rstudio-connect" = source("/home/data-science/data_science/global_vars.R",echo = T),
  source("~/data_science/global_vars.R",echo = T)
)

.topic = 'prod-data-science-uploads'
.url = 'https://kafka-rest-proxy.flipside.systems'
.project = 'nft_deal_score_projections'
.data = NULL

.key <- paste(.project,as.character(round(as.numeric(Sys.time()))),sep = "-")

.data <- data.table(
	collection = c('Solana Monkey Business', 'Solana Monkey Business')
	, token_id = c(1, 2)
	, mint_address = c('2sQtGiaXdKAayVbSPExBJVLp3ymnd8QmdKFBY4B7AVmJ', 'Esv9qkvaqFudHFSXk82pAxikReGo49nR63PAnLMbSjMf')
	, fair_market_price = c( 220, 218 )
)

if(nrow(.data) <= 100){
	#unbox everything to encode in proper json
	unboxed.body <- jsonlite::toJSON(.data,dataframe = "rows",auto_unbox = TRUE)
	.body <- paste0(
		'{"records": [{"key": "',.key,'","value":',unboxed.body,'}]}',
		collapse = ""
	)
	httr::POST(url = paste(.url,"topics",.topic,sep = "/"),
				add_headers('Content-Type' = "application/vnd.kafka.json.v2+json",
							'Accept' = "application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json"),
				body = .body)
} else {
	message("Body has more than 100 rows: cunking!")
	chunks <- data.table(
	start = seq(1,nrow(.data),100),
	end = c(seq(100,nrow(.data)-1,100),nrow(.data))
	)
	chunkprog <- txtProgressBar(max = nrow(chunks),style = 3)
	for(chunk in 1:nrow(chunks)) {
	setTxtProgressBar(chunkprog, chunk)
	start <- chunks[chunk]$start
	end <- chunks[chunk]$end
	unboxed.body <- jsonlite::toJSON(.data[start:end],
										dataframe = "rows",
										auto_unbox = TRUE)
	.body <- paste0(
		'{"records": [{"key": "',.key,'","value":',unboxed.body,'}]}',
		collapse = ""
	)
	httr::POST(url = paste(.url,"topics",.topic,sep = "/"),
				add_headers('Content-Type' = "application/vnd.kafka.json.v2+json",
							'Accept' = "application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json"),
				body = .body)
	}
}