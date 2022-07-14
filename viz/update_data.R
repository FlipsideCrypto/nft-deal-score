# install.packages('reticulate')
library(reticulate)
library(httr)
library(jsonlite)


.topic = 'prod-nft-metadata-uploads'
.key = 'solana-nft-metadata'
.url = 'https://kafka-rest-proxy.flipside.systems'

user <- Sys.info()[['user']]
isRstudio <- user %in% c('rstudio-connect','data-science')

base_dir <- ifelse(
	isRstudio
	, '/rstudio-data/'
	, '~/git/nft-deal-score/viz/'
)

if(isRstudio) {
	source('/home/data-science/data_science/util/util_functions.R')
	source_python('/home/data-science/data_science/viz/nft-deal-score/upload_solana_nft_labels.py')
} else {
	source('~/data_science/util/util_functions.R')
	source_python(paste0(base_dir, 'upload_solana_nft_labels.py'))
}


#########################
#     Load NFT Data     #
#########################
mints_from_me()
pull_from_metaboss()
how_rare_is_api()
compile()



###############################
#     Upload NFT Metadata     #
###############################
files <- list.files(paste0(base_dir, 'nft_labels/metadata/results/'))
it <- 0
for(f in files) {
	print(f)
	results <- read.csv(paste0(base_dir,'/nft_labels/metadata/results/',f))
	for(r in results$results) {
		it <- it + 1
		print(paste0('#',it))
			out <- tryCatch(
			{
				# s <- readChar(fileName, file.info(fileName)$size)
				s <- r
				.body <- paste0(
					'{"records": [{"key": "',.key,'","value":',s,'}]}',
					collapse = ""
				)
				r <- httr::POST(url = paste(.url,"topics",.topic,sep = "/"),
							add_headers('Content-Type' = "application/vnd.kafka.json.v2+json",
										'Accept' = "application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json"),
							body = .body)
				print(r)
			},
			error=function(cond) {
				print(cond)
				return(NA)
			},
			warning=function(cond) {
				print(cond)
				return(NULL)
			},
			finally={
			}
		)
	}
}

