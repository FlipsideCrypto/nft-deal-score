server <- function(input, output, session) {
	# load('data.Rdata')
	user <- Sys.info()[['user']]
	# options(warn=-1)

	base_dir <- ifelse(
		user == 'rstudio-connect'
		, '/rstudio-data/'
		, ifelse(user == 'fcaster'
			, '/srv/shiny-server/nft-deal-score/'
			, '~/git/nft-deal-score/viz/'
		)
	)
	# base_dir <- '/srv/shiny-server/nft-deal-score/'

	load(paste0(base_dir, 'nft_deal_score_data.RData'))
	load(paste0(base_dir, 'nft_deal_score_listings_data.RData'))
	load(paste0(base_dir, 'nft_deal_score_sales_data.RData'))

    metadata <- unique(attributes[, list(collection, feature_name, feature_value)])

	SD_MULT = 3
	SD_SCALE = 1.95

	with_tooltip <- function(value, tooltip) {
		div(style = "text-decoration: underline; text-decoration-style: dotted; cursor: help",
		tippy(value, tooltip))
	}

	getFloors <- reactive({
		selected <- getCollection()
		floor_1 <- as.numeric(input$floorprice)
		floor_0 <- coefsdf[ collection == eval(selected) ]$floor_price[1]
		if (length(floor_0) == 0 | is.na(floor_0)) {
			l <- c(1, 1)
			return(l)
		}
		if (length(floor_1) == 0) {
			floor_1 <- floor_0
		} else if (is.na(floor_1)) {
			floor_1 <- floor_0
		}
		floors <- c( floor_0, floor_1 )
		return(floors)
	})

	output$floorpriceinput <- renderUI({
		selected <- getCollection()
		if( selected == '' ) {
			return(NULL)
		}
		textInput(
			inputId = 'floorprice'
			, label = NULL
			, width = "100%"
			, placeholder = coefsdf[ collection == eval(selected) ]$floor_price[1]
			, value = min(listings[ collection == eval(selected) ]$price)
		)
	})

	output$loandaysinput <- renderUI({
		selected <- getCollection()
		if( selected == '' ) {
			return(NULL)
		}
		textInput(
			inputId = 'loandays'
			, label = NULL
			, width = "100%"
		)
	})
	output$loanamountinput <- renderUI({
		selected <- getCollection()
		if( selected == '' ) {
			return(NULL)
		}
		textInput(
			inputId = 'loanamount'
			, label = NULL
			, width = "100%"
		)
	})
	output$loanreturninput <- renderUI({
		selected <- getCollection()
		if( selected == '' ) {
			return(NULL)
		}
		textInput(
			inputId = 'loanreturn'
			, label = NULL
			, width = "100%"
		)
	})

	output$maxpriceinput <- renderUI({
		textInput(
			inputId = 'maxprice'
			, label = NULL
			, width = "100%"
		)
	})

	output$maxnftrankinput <- renderUI({
		textInput(
			inputId = 'maxnftrank'
			, label = NULL
			, width = "100%"
		)
	})

	output$maxrarityrankinput <- renderUI({
		textInput(
			inputId = 'maxrarityrank'
			, label = NULL
			, width = "100%"
		)
	})

	output$maxnftrankinput2 <- renderUI({
		textInput(
			inputId = 'maxnftrank2'
			, label = NULL
			, width = "100%"
		)
	})
	output$minnftrankinput2 <- renderUI({
		textInput(
			inputId = 'minnftrank2'
			, label = NULL
			, width = "100%"
		)
	})

	output$minfloorinput <- renderUI({
		textInput(
			inputId = 'minfloorinput'
			, label = NULL
			, width = "100%"
		)
	})
	output$maxfloorinput <- renderUI({
		textInput(
			inputId = 'maxfloorinput'
			, label = NULL
			, width = "100%"
		)
	})
	output$maxrarityrankinput2 <- renderUI({
		textInput(
			inputId = 'maxrarityrank2'
			, label = NULL
			, width = "100%"
		)
	})
	output$minrarityrankinput2 <- renderUI({
		textInput(
			inputId = 'minrarityrank2'
			, label = NULL
			, width = "100%"
		)
	})

	output$filter1select <- renderUI({
		selected <- getCollection()
        name <- getMetadataColumns()
        if(length(name) < 1) {
            return(NULL)
        }
        name <- name[1]
        m <- metadata[ collection == eval(selected) & feature_name == eval(name) ]
		choices <- c('Any', sort(m$feature_value))
		selectInput(
			inputId = 'filter1'
			, label = NULL
			, selected = 'Any'
			, choices = choices
			, width = "100%"
		)
	})
	output$filter2select <- renderUI({
		selected <- getCollection()
        name <- getMetadataColumns()
        if(length(name) < 2) {
            return(NULL)
        }
        name <- name[2]
        m <- metadata[ collection == eval(selected) & feature_name == eval(name) ]
		choices <- c('Any', sort(m$feature_value))
		selectInput(
			inputId = 'filter2'
			, label = NULL
			, selected = 'Any'
			, choices = choices
			, width = "100%"
		)
	})
	output$filter3select <- renderUI({
		selected <- getCollection()
        name <- getMetadataColumns()
        if(length(name) < 3) {
            return(NULL)
        }
        name <- name[3]
        m <- metadata[ collection == eval(selected) & feature_name == eval(name) ]
		choices <- c('Any', sort(m$feature_value))
		selectInput(
			inputId = 'filter3'
			, label = NULL
			, selected = 'Any'
			, choices = choices
			, width = "100%"
		)
	})
	output$filter4select <- renderUI({
		selected <- getCollection()
        name <- getMetadataColumns()
        if(length(name) < 4) {
            return(NULL)
        }
        name <- name[4]
        m <- metadata[ collection == eval(selected) & feature_name == eval(name) ]
		choices <- c('Any', sort(m$feature_value))
		selectInput(
			inputId = 'filter4'
			, label = NULL
			, selected = 'Any'
			, choices = choices
			, width = "100%"
		)
	})
	output$filter5select <- renderUI({
		selected <- getCollection()
        name <- getMetadataColumns()
        if(length(name) < 5) {
            return(NULL)
        }
        name <- name[5]
        m <- metadata[ collection == eval(selected) & feature_name == eval(name) ]
		choices <- c('Any', sort(m$feature_value))
		selectInput(
			inputId = 'filter5'
			, label = NULL
			, selected = 'Any'
			, choices = choices
			, width = "100%"
		)
	})
	output$filter6select <- renderUI({
		selected <- getCollection()
        name <- getMetadataColumns()
        if(length(name) < 6) {
            return(NULL)
        }
        name <- name[6]
        m <- metadata[ collection == eval(selected) & feature_name == eval(name) ]
		choices <- c('Any', sort(m$feature_value))
		selectInput(
			inputId = 'filter6'
			, label = NULL
			, selected = 'Any'
			, choices = choices
			, width = "100%"
		)
	})
	output$filter7select <- renderUI({
		selected <- getCollection()
        name <- getMetadataColumns()
        if(length(name) < 7) {
            return(NULL)
        }
        name <- name[7]
        m <- metadata[ collection == eval(selected) & feature_name == eval(name) ]
		choices <- c('Any', sort(m$feature_value))
		selectInput(
			inputId = 'filter7'
			, label = NULL
			, selected = 'Any'
			, choices = choices
			, width = "100%"
		)
	})
	output$filter8select <- renderUI({
		selected <- getCollection()
        name <- getMetadataColumns()
        if(length(name) < 8) {
            return(NULL)
        }
        name <- name[8]
        m <- metadata[ collection == eval(selected) & feature_name == eval(name) ]
		choices <- c('Any', sort(m$feature_value))
		selectInput(
			inputId = 'filter8'
			, label = NULL
			, selected = 'Any'
			, choices = choices
			, width = "100%"
		)
	})
	output$filter9select <- renderUI({
		selected <- getCollection()
        name <- getMetadataColumns()
        if(length(name) < 9) {
            return(NULL)
        }
        name <- name[9]
        m <- metadata[ collection == eval(selected) & feature_name == eval(name) ]
		choices <- c('Any', sort(m$feature_value))
		selectInput(
			inputId = 'filter9'
			, label = NULL
			, selected = 'Any'
			, choices = choices
			, width = "100%"
		)
	})
	output$filter10select <- renderUI({
		selected <- getCollection()
        name <- getMetadataColumns()
        if(length(name) < 10) {
            return(NULL)
        }
        name <- name[10]
        m <- metadata[ collection == eval(selected) & feature_name == eval(name) ]
		choices <- c('Any', sort(m$feature_value))
		selectInput(
			inputId = 'filter10'
			, label = NULL
			, selected = 'Any'
			, choices = choices
			, width = "100%"
		)
	})
	output$filter11select <- renderUI({
		selected <- getCollection()
        name <- getMetadataColumns()
        if(length(name) < 11) {
            return(NULL)
        }
        name <- name[11]
        m <- metadata[ collection == eval(selected) & feature_name == eval(name) ]
		choices <- c('Any', sort(m$feature_value))
		selectInput(
			inputId = 'filter11'
			, label = NULL
			, selected = 'Any'
			, choices = choices
			, width = "100%"
		)
	})
	output$filter12select <- renderUI({
		selected <- getCollection()
        name <- getMetadataColumns()
        if(length(name) < 12) {
            return(NULL)
        }
        name <- name[12]
        m <- metadata[ collection == eval(selected) & feature_name == eval(name) ]
		choices <- c('Any', sort(m$feature_value))
		selectInput(
			inputId = 'filter12'
			, label = NULL
			, selected = 'Any'
			, choices = choices
			, width = "100%"
		)
	})
	output$filter13select <- renderUI({
		selected <- getCollection()
        name <- getMetadataColumns()
        if(length(name) < 13) {
            return(NULL)
        }
        name <- name[13]
        m <- metadata[ collection == eval(selected) & feature_name == eval(name) ]
		choices <- c('Any', sort(m$feature_value))
		selectInput(
			inputId = 'filter13'
			, label = NULL
			, selected = 'Any'
			, choices = choices
			, width = "100%"
		)
	})
	output$filter14select <- renderUI({
		selected <- getCollection()
        name <- getMetadataColumns()
        if(length(name) < 14) {
            return(NULL)
        }
        name <- name[14]
        m <- metadata[ collection == eval(selected) & feature_name == eval(name) ]
		choices <- c('Any', sort(m$feature_value))
		selectInput(
			inputId = 'filter14'
			, label = NULL
			, selected = 'Any'
			, choices = choices
			, width = "100%"
		)
	})
	output$filter15select <- renderUI({
		selected <- getCollection()
        name <- getMetadataColumns()
        if(length(name) < 15) {
            return(NULL)
        }
        name <- name[15]
        m <- metadata[ collection == eval(selected) & feature_name == eval(name) ]
		choices <- c('Any', sort(m$feature_value))
		selectInput(
			inputId = 'filter15'
			, label = NULL
			, selected = 'Any'
			, choices = choices
			, width = "100%"
		)
	})
	output$filter16select <- renderUI({
		selected <- getCollection()
        name <- getMetadataColumns()
        if(length(name) < 16) {
            return(NULL)
        }
        name <- name[16]
        m <- metadata[ collection == eval(selected) & feature_name == eval(name) ]
		choices <- c('Any', sort(m$feature_value))
		selectInput(
			inputId = 'filter16'
			, label = NULL
			, selected = 'Any'
			, choices = choices
			, width = "100%"
		)
	})
	output$filter17select <- renderUI({
		selected <- getCollection()
        name <- getMetadataColumns()
        if(length(name) < 17) {
            return(NULL)
        }
        name <- name[17]
        m <- metadata[ collection == eval(selected) & feature_name == eval(name) ]
		choices <- c('Any', sort(m$feature_value))
		selectInput(
			inputId = 'filter17'
			, label = NULL
			, selected = 'Any'
			, choices = choices
			, width = "100%"
		)
	})
	output$filter18select <- renderUI({
		selected <- getCollection()
        name <- getMetadataColumns()
        if(length(name) < 18) {
            return(NULL)
        }
        name <- name[18]
        m <- metadata[ collection == eval(selected) & feature_name == eval(name) ]
		choices <- c('Any', sort(m$feature_value))
		selectInput(
			inputId = 'filter18'
			, label = NULL
			, selected = 'Any'
			, choices = choices
			, width = "100%"
		)
	})

	output$collectionselect <- renderUI({
		choices <- sort(unique(pred_price$collection))
		selectInput(
			inputId = 'collectionname'
			, label = NULL
			, selected = 'Catalina Whale Mixer'
			, choices = choices
			, width = "100%"
		)
	})
observeEvent(input$collectionname,
{
	cur_selected <- getCollection()
	choices <- sort(pred_price[collection == eval(cur_selected)]$token_id)
	updateSelectizeInput(session, 'tokenid', choices = choices, server = TRUE)
})

updateSelectizeInput(session, "listid", choices = c(1, 2, 3, 4), server = T)
# cur_selected <- getCollection()
# choices <- sort(pred_price[collection == eval(cur_selected)]$token_id)
# choices <- sort(unique(pred_price$token_id))
choices <- seq(1:10000)
updateSelectizeInput(session, 'tokenid', choices = choices, server = TRUE)

	output$nftselect <- renderUI({
		selected <- getCollection()
		if( selected == '' ) {
			return(NULL)
		}
		choices <- sort(pred_price[collection == eval(selected)]$token_id)
		updateSelectizeInput(session, 'nftselect', choices = choices, server = TRUE)
		# selectInput(
		# selectizeInput(
		# 	inputId = 'tokenid'
		# 	, label = NULL
		# 	, choices = choices
		# 	, width = "100%"
		# 	, options = list(maxOptions = 10)
		# 	# , size = 50
		# 	# , selectize = FALSE
		# )
		# updateSelectizeInput(
		# 	session
		# 	, 'tokenid'
		# 	, label = NULL
		# 	, choices = choices
		# 	, width = "100%"
		# 	, server = TRUE
		# )
	})
	getTokenId <- reactive({
		if(length(input$tokenid) == 0) {
			return('')
		}
		return(input$tokenid)
	})
	getCollection <- reactive({
		if(length(input$collectionname) == 0) {
			return('')
		}
		return(input$collectionname)
	})
	getChain <- reactive({
		selected <- getCollection()
		if(selected == '') {
			return('')
		}
		chain <- tokens[collection == eval(selected) ]
		if(nrow(chain) == 0) {
			return('')
		}
		return(chain$chain[1])
	})

	output$tokenid <- renderText({
		id <- getTokenId()
		chain <- getChain()
		t <- ""
		selected <- getCollection()
		if( length(id) == 0 | selected == '' ) {
			return(t)
		}
		title <- ifelse(
			selected == 'Aurory'
			, 'Aurorian', ifelse(
				selected == 'Thugbirdz'
				, 'THUG'
				, ifelse(
					selected %in% c('Solana Monkey Business') | chain == 'Ethereum'
					, selected
					, ifelse(
						selected %in% c('Cets on Creck')
						, strsplit(selected, ' |s ')[[1]][1]
						, ifelse(
							selected %in% c('Stoned Ape Crew', 'Catalina Whale Mixer')
							, paste(strsplit(selected, ' ')[[1]][1], strsplit(selected, ' ')[[1]][2], sep = ' ')
							, substr(selected, 1, nchar(selected) - 1)
						)
					)
				)
			)
		)
		if (!is.na(id)) {
			t <- paste0(title," #", id)
		}
		paste0(t)
	})

	output$tokenrank <- renderText({
		id <- getTokenId()
		selected <- getCollection()
		t <- ""
		if( length(id) == 0 | selected == '' ) {
			return(t)
		}
		if (!is.na(id) & !is.na(selected)) {
			cur_0 <- pred_price[collection == eval(selected) ]
			cur_1 <- cur_0[ token_id == eval(id) ]
			if (nrow(cur_1)) {
				t <- paste0("Deal Score Rank #", format(cur_1$rk[1], big.mark=",")," / ",format(nrow(cur_0), big.mark=","))
			}
		}
		paste0(t)
	})

	output$salesAverage <- renderText({
        data <- getSalesData()
		if(length(data) == 0) {
			return(NULL)
		}
        t <- ''
        if (nrow(data)) {
			data <- head(data, 100)
            p <- format(round(mean(data$price), 1), big.mark=',')
            f <- format(round(mean(data$vs_floor), 1), big.mark=',')
			data[, pct_vs_floor := (vs_floor + price) / price ]
			pct <- sum(data$price) / sum(data$mn_20)
            pct <- format(round(pct, 1), big.mark=',')
			chain <- getChain()
			currency <- ifelse( chain == 'Solana', 'SOL', ifelse(chain == 'Ethereum', 'ETH', 'LUNA') )
            t <- paste0(p, ' $',currency,' (+',f,' / ',pct,'x vs the floor)')
        }
		paste0(t)
	})

	output$rarityrank <- renderText({
		id <- getTokenId()
		selected <- getCollection()
		chain <- getChain()
		t <- ""
		if( length(id) == 0 | selected == '' ) {
			return(t)
		}
		if (!is.na(id) & !is.na(selected)) {
			cur_0 <- pred_price[collection == eval(selected) ]
			cur_1 <- cur_0[ token_id == eval(id) ]
			if (nrow(cur_1)) {
                a <- ifelse( 
					chain == 'Solana'
					, 'HowRareIs'
					, ifelse(
						selected %in% c('Levana Dragon Eggs')
						, 'Collection'
						, ifelse(
							selected %in% c('Galactic Angels') | chain == 'Ethereum'
							, 'Rarity'
							, 'NotFoundTerra'
						)
					)
				)
				t <- paste0(a, " Rank #", format(cur_1$nft_rank[1], big.mark=",")," / ",format(nrow(cur_0), big.mark=","))
			}
		}
		paste0(t)
	})

	output$hrirank <- renderText({
		id <- getTokenId()
		id <- getTokenId()
		selected <- getCollection()
		chain <- getChain()
		t <- ""
		if (length(id) == 0 | selected == '' | length(chain) == 0) {
			return(t)
		}
		if ( chain == 'Solana' & !is.na(id) & !is.na(selected)) {
			cur_0 <- pred_price[collection == eval(selected) ]
			cur_1 <- cur_0[ token_id == eval(as.numeric(input$tokenid)) ]
			if (nrow(cur_1)) {
				t <- paste0("howrare.is rank: ", format(cur_1$hri_rank[1], big.mark=","))
			}
		}
		paste0(t)
	})

	getConvertedPrice <- reactive({
		selected <- getCollection()
		if( selected == '' ) {
			return(NULL)
		}
		log_coef <- coefsdf[ collection == eval(selected) ]$log_coef[1]
		lin_coef <- coefsdf[ collection == eval(selected) ]$lin_coef[1]
		tot <- log_coef + lin_coef
		log_coef <- log_coef / tot
		lin_coef <- lin_coef / tot

		floors <- getFloors()

		abs_chg <- (floors[2] - floors[1]) * lin_coef
		pct_chg <- (floors[2] - floors[1]) * log_coef
		tuple <- c( abs_chg, pct_chg )
		return(tuple)
	})

	adjust_price <- function(p_0, tuple) {
		floors <- getFloors()
		p_1 <- p_0 + tuple[1] + (tuple[2] * p_0 / floors[1] )
		p_1 <- pmax(floors[2], p_1)
		return(p_1)
	}

	output$loanscore <- renderText({
		paste0('Loan Score: ')
	})
	output$fairmarketprice <- renderText({
		id <- getTokenId()
		selected <- getCollection()

		t <- ""
		if( id == '' ) {
			return(t)
		}
		if ( !is.na(id) ) {
			cur <- pred_price[ token_id == eval(as.numeric(input$tokenid)) & collection == eval(selected) ]
			p_0 <- cur$pred_price[1]
			tuple <- getConvertedPrice()
			p_1 <- adjust_price(p_0, tuple)
			chain <- getChain()
			currency <- ifelse( chain == 'Solana', 'SOL', 
				ifelse( chain == 'Ethereum', 'ETH', 'LUNA' )
			)
			if (nrow(cur)) {
				t <- paste0("Fair Market Price: ", (format(p_1, digits=3, decimal.mark=".",big.mark=",")), " ", currency)
			}
		}
		paste0(t)
	})

	output$tokenimg <- renderUI({
		id <- getTokenId()
		selected <- getCollection()
		if (length(id) == 0 | selected == '') {
			return(NULL)
		}
		src <- tokens[ (collection == eval(selected)) & (token_id == eval(id)) ]$image_url[1]
		t <- tags$img(src = src)
		t
	})

	getAttributesTable <- reactive({
		id <- getTokenId()
		selected <- getCollection()
		if( length(id) == 0 | selected == '' ) {
			return(head(attributes, 0))
		}
		cur <- attributes[ token_id == eval(as.numeric(id)) & collection == eval(selected) ]
		cur <- merge( cur, feature_values[collection == eval(selected), list(feature_name, feature_value, pct_vs_baseline) ], all.x=TRUE, by=c('feature_name','feature_value') )
		cur <- cur[order(rarity)]
		# floor <- getFloors()[2]
		# log_coef <- coefsdf[ collection == eval(selected) ]$log_coef[1]
		# lin_coef <- coefsdf[ collection == eval(selected) ]$lin_coef[1]
		# s <- sum(cur$pct_vs_baseline)
		# p <- getPredPrice()
		# p <- as.numeric(p[ token_id == eval(as.numeric(id)) ]$pred_price)
		# # p <- pred_price[ token_id == eval(as.numeric(id)) & collection == eval(selected) ]$pred_price
		# ratio <- (p / floor) - 1
		# ratio <- pmax(0, ratio)
		# if (ratio > 0 & length(ratio) > 0) {
		# 	mult <- ratio / s
		# 	cur[, pct_vs_baseline := pct_vs_baseline * eval(mult) ]
		# }
		# cur[, vs_baseline := 0 ]
		# cur[, pred_vs_baseline := 0 ]
		# cur[, vs_baseline := 0 ]
		# cur[, vs_baseline := round((pred_vs_baseline * eval(lin_coef)) + (pct_vs_baseline * eval(floor) * eval(log_coef) ), 1) ]
		# cur[, pred_vs_baseline := round(pred_vs_baseline, 1) ]
		# cur[, vs_baseline := round(pred_vs_baseline + (pct_vs_baseline * eval(floor)), 1) ]
		return(cur)
	})

	output$attributestable <- renderReactable({
		data <- getAttributesTable()
		if( nrow(data) == 0 ) {
			return(NULL)
		}
		data[, rarity := ifelse(is.na(rarity), '', paste0(format(round(rarity*100, 2), digits=4, decimal.mark="."),'%') )]

		# reactable(data[, list( feature, value, rarity, vs_baseline, pred_vs_baseline, pct_vs_baseline )],
		# data <- data[, list( feature, value, rarity, pct_vs_baseline )]
		data <- data[, list( feature_name, feature_value, rarity, pct_vs_baseline )]
		data[, pct_vs_baseline := ifelse( is.na(pct_vs_baseline), '', paste0('+', format(round(pct_vs_baseline*1000)/10, digits=4, decimal.mark=".", big.mark=",", trim = T), '%') ) ]
		reactable(data,
			defaultColDef = colDef(
				headerStyle = list(background = "#10151A")
			),
			defaultPageSize = 5,
			borderless = TRUE,
			outlined = FALSE,
			columns = list(
				feature_name = colDef(name = "Attribute", align = "left"),
				feature_value = colDef(name = "Name", align = "left"),
				rarity = colDef(name = "Rarity", align = "left"),
				pct_vs_baseline = colDef(
					name="General Price Impact", header=with_tooltip("General Price Impact", "The estimated price impact of this feature vs the floor")
					, html = TRUE
					, align = "left"
					, cell = function(x) {
						htmltools::tags$span(x)
					}
				)
			)
	    )
	})

	output$featurestable <- renderReactable({
		selected <- getCollection()
		if( selected == '' ) {
			return(NULL)
		}
		data <- feature_values[ collection == eval(selected)]
		reactable(data[, list( feature_name, feature_value, rarity, pct_vs_baseline )],
			defaultColDef = colDef(
				headerStyle = list(background = "#10151A")
			),
			borderless = TRUE,
			outlined = FALSE,
			searchable = FALSE,
			columns = list(
				feature_name = colDef(name = "Attribute", align = "left"),
				feature_value = colDef(name = "Value", align = "left"),
				rarity = colDef(name = "Rarity", align = "left", cell = function(x) {
					htmltools::tags$span(paste0(format(x*100, digits=3, decimal.mark=".", big.mark=","),'%'))
				}),
				pct_vs_baseline = colDef(name = "$ Value", align = "left", cell = function(x) {
					htmltools::tags$span(paste0('+', format(x*100, digits=3, decimal.mark=".", big.mark=","), '%'))
				})
			)
	    )
	})

	getPredPrice <- reactive({
		selected <- getCollection()
		if (selected == '') {
			return(t)
		}
		data <- pred_price[ collection == eval(selected), list( token_id, rk, pred_price )]
		tuple <- getConvertedPrice()
		floors <- getFloors()
		data[, pred_price := pred_price + eval(tuple[1]) + ( eval(tuple[2]) * pred_price / eval(floors[1]) ) ]
		data[, pred_price := pmax( eval(floors[2]), pred_price) ]
		return(data)
	})

	output$nftstable <- renderReactable({
		selected <- getCollection()
		if( selected == '' ) {
			return(NULL)
		}
		data <- getPredPrice()
		data[, pred_price := paste0(format(pred_price, digits=3, decimal.mark=".", big.mark=",")) ]
		reactable(data,
			defaultColDef = colDef(
				headerStyle = list(background = "#10151A")
			),
			borderless = TRUE,
			outlined = FALSE,
			searchable = TRUE,
			columns = list(
				token_id = colDef(name = "Token ID", align = "left"),
				rk = colDef(name = "Rank", align = "left"),
				pred_price = colDef(name = "Fair Market Price", align = "left")
			)
	    )
	})

    getFilteredSalesData <- function(data, selected, val, i) {
        if(length(val) > 0) {
            if(val != 'Any') {
                att <- getMetadataColumns()
                if(length(att) >= i) {
                    att <- att[i]
                    include <- attributes[collection == eval(selected) & feature_name == eval(att) & feature_value == eval(val), list(token_id) ]
                    data <- merge(data, include, by=c('token_id'))
                }
            }
        }
        return(data)
    }

    getSalesDataFn <- function(selected, sales, tokens, pred_price, attributes) {
		data <- sales[ collection == eval(selected)]
		m <- pred_price[collection == eval(selected), list(token_id, rk)]
		data <- merge(data, m, all.x=TRUE)

		data <- merge(data, tokens[collection == eval(selected), list(collection, token_id, image_url)], all.x=T )
		data <- data[, list( token_id, image_url, block_timestamp, price, pred, mn_20, rk, nft_rank )]

        data <- data[order(-block_timestamp)]
		if(nrow(data) == 0) {
			return(data)
		}

        data[, vs_floor := pmax(0, price - mn_20) ]

        m <- dcast(attributes[collection == eval(selected), list(token_id, feature_name, feature_value)], token_id ~ feature_name, value.var='feature_value')
        names <- colnames(m)
        data <- merge(data, m, all.x=TRUE)


        data <- data[order(-block_timestamp)]
        data[, mn_20 := pmin(mn_20, price) ]
        data[, mn_20_label := paste0(format(round(mn_20, 1), scientific = FALSE, digits=2, decimal.mark=".", big.mark=","))]
        data[, price_label := paste0(format(price, scientific = FALSE, digits=2, decimal.mark=".", big.mark=","))]
        data[, block_timestamp := substr(block_timestamp, 1, 10) ]
        return(data)
    }

    getSalesData <- reactive({
		selected <- getCollection()
		if( selected == '' ) {
			return(NULL)
		}
		# data <- sales[ collection == eval(selected) , list( token_id, block_timestamp, price, pred, mn_20 )]
		data <- sales[ collection == eval(selected)]

		coefsdf[, tot := lin_coef + log_coef ]
		coefsdf[, lin_coef := lin_coef / tot]
		coefsdf[, log_coef := log_coef / tot]

		fmp <- merge( pred_price, coefsdf, by=c('collection') )
		fmp <- merge( fmp, data[, list(token_id, collection, block_timestamp, price, mn_20)], by=c('token_id','collection') )
		# fmp <- merge( fmp, floors, by=c('collection') )
		fmp[, abs_chg := (mn_20 - floor_price) * lin_coef ]
		fmp[, pct_chg := (mn_20 - floor_price) * log_coef ]
		fmp[, pred := pred_price + abs_chg + (pct_chg * pred_price / floor_price) ]
		# fmp[, fair_market_price := pred_price + abs_chg + (pct_chg * pred_price / floor_price) ]
		data <- fmp

		# m <- pred_price[collection == eval(selected), list(token_id, rk)]
		# data <- merge(data, m, all.x=TRUE, by=c('token_id'))
		# data[token_id == 5144]
		# m[token_id == 5144]
		if(nrow(data) == 0) {
			return(data.table())
		}

        if(input$maxnftrank2 != '') {
            r <- as.numeric(input$maxnftrank2)
            data <- data[ rk <= eval(r) ]
        }
        if(input$minnftrank2 != '') {
            data <- data[ rk >= eval(as.numeric(input$minnftrank2)) ]
        }
        if(input$maxrarityrank2 != '') {
            r <- as.numeric(input$maxrarityrank2)
            data <- data[ nft_rank <= eval(r) ]
        }
        if(input$minfloorinput != '') {
            r <- as.numeric(input$minfloorinput)
            data <- data[ mn_20 >= eval(r) ]
        }
        if(input$maxfloorinput != '') {
            r <- as.numeric(input$maxfloorinput)
            data <- data[ mn_20 <= eval(r) ]
        }
        if(input$maxrarityrank2 != '') {
            r <- as.numeric(input$maxrarityrank2)
            data <- data[ nft_rank <= eval(r) ]
        }
        if(input$minrarityrank2 != '') {
            data <- data[ nft_rank >= eval(as.numeric(input$minrarityrank2)) ]
        }
        data <- getFilteredSalesData(data, selected, input$filter1, 1)
        data <- getFilteredSalesData(data, selected, input$filter2, 2)
        data <- getFilteredSalesData(data, selected, input$filter3, 3)
        data <- getFilteredSalesData(data, selected, input$filter4, 4)
        data <- getFilteredSalesData(data, selected, input$filter5, 5)
        data <- getFilteredSalesData(data, selected, input$filter6, 6)
        data <- getFilteredSalesData(data, selected, input$filter7, 7)
        data <- getFilteredSalesData(data, selected, input$filter8, 8)
        data <- getFilteredSalesData(data, selected, input$filter9, 9)
        data <- getFilteredSalesData(data, selected, input$filter10, 10)
        data <- getFilteredSalesData(data, selected, input$filter11, 11)
        data <- getFilteredSalesData(data, selected, input$filter12, 12)
        data <- getFilteredSalesData(data, selected, input$filter13, 13)
        data <- getFilteredSalesData(data, selected, input$filter14, 14)
        data <- getFilteredSalesData(data, selected, input$filter15, 15)
        data <- getFilteredSalesData(data, selected, input$filter16, 16)
        data <- getFilteredSalesData(data, selected, input$filter17, 17)
        data <- getFilteredSalesData(data, selected, input$filter18, 18)
        data <- getFilteredSalesData(data, selected, input$filter19, 19)
        data <- getFilteredSalesData(data, selected, input$filter20, 20)

		data <- merge(data, tokens[collection == eval(selected), list(collection, token_id, image_url)], all.x=T, by=c('collection','token_id') )
		data <- data[, list( token_id, image_url, block_timestamp, price, pred, mn_20, rk, nft_rank )]

        data <- data[order(-block_timestamp)]

        data[, vs_floor := pmax(0, price - mn_20) ]

        m <- dcast(attributes[collection == eval(selected), list(token_id, feature_name, feature_value)], token_id ~ feature_name, value.var='feature_value')
        names <- colnames(m)
        data <- merge(data, m, all.x=TRUE, by=c('token_id'))


        data <- data[order(-block_timestamp)]
        data[, mn_20 := pmin(mn_20, price) ]
        data[, block_timestamp := substr(block_timestamp, 1, 10) ]
        return(data)
    })

    getMetadataColumns <- reactive({
        selected <- getCollection()
        m <- unique(metadata[ collection == eval(selected), list(feature_name) ])
		names <- sort(m$feature_name)
        return(names)
    })

    getFilterText <- function(i) {
		t <- ''
        m <- getMetadataColumns()
        if(length(m) >= i) {
            t <- m[i]
        }
        return(t)
    }


	output$filter1 <- renderText({
		paste0(getFilterText(1))
	})
	output$filter2 <- renderText({
		paste0(getFilterText(2))
	})
	output$filter3 <- renderText({
		paste0(getFilterText(3))
	})
	output$filter4 <- renderText({
		paste0(getFilterText(4))
	})
	output$filter5 <- renderText({
		paste0(getFilterText(5))
	})
	output$filter6 <- renderText({
		paste0(getFilterText(6))
	})
	output$filter7 <- renderText({
		paste0(getFilterText(7))
	})
	output$filter8 <- renderText({
		paste0(getFilterText(8))
	})
	output$filter9 <- renderText({
		paste0(getFilterText(9))
	})
	output$filter10 <- renderText({
		paste0(getFilterText(10))
	})
	output$filter11 <- renderText({
		paste0(getFilterText(11))
	})
	output$filter12 <- renderText({
		paste0(getFilterText(12))
	})
	output$filter13 <- renderText({
		paste0(getFilterText(13))
	})
	output$filter14 <- renderText({
		paste0(getFilterText(14))
	})
	output$filter15 <- renderText({
		paste0(getFilterText(15))
	})
	output$filter16 <- renderText({
		paste0(getFilterText(16))
	})
	output$filter17 <- renderText({
		paste0(getFilterText(17))
	})
	output$filter18 <- renderText({
		paste0(getFilterText(18))
	})
	output$filter19 <- renderText({
		paste0(getFilterText(19))
	})
	output$filter20 <- renderText({
		paste0(getFilterText(20))
	})

	output$salestable <- renderReactable({
		selected <- getCollection()
		if( selected == '' ) {
			return(NULL)
		}
        data <- getSalesData()
		if(nrow(data) == 0) {
			return(NULL)
		}

        data[, mn_20 := paste0(format(round(mn_20, 1), scientific = FALSE, digits=2, decimal.mark=".", big.mark=","))]
        data[, price := paste0(format(round(price, 1), scientific = FALSE, digits=2, decimal.mark=".", big.mark=","))]
        data[, pred := paste0(format(round(pred, 1), scientific = FALSE, digits=2, decimal.mark=".", big.mark=","))]
		data[, vs_floor := NULL ]
        # data <- future(getSalesDataFn(selected, sales, tokens, pred_price, attributes)) %...>% 
		reactable(data, 
			defaultColDef = colDef(
				headerStyle = list(background = "#10151A")
			),
			# filterable = TRUE,
			borderless = TRUE,
			outlined = FALSE,
			searchable = FALSE,
			columns = list(
				token_id = colDef(name = "Token ID", align = "left"),
				image_url = colDef(name = "Token", align = "left", cell = function(value, index) {
					if(index <= 100) {
						htmltools::tags$img(src=value)
					} else {
						return(NULL)
					}
				}),
				block_timestamp = colDef(name = "Sale Date", align = "left"),
				price = colDef(name = "Price", align = "left"),
				pred = colDef(name = "Fair Market Price", align = "left"),
				rk = colDef(name = "Deal Score Rank", align = "left"),
				nft_rank = colDef(name = "Rarity Rank", align = "left"),
				mn_20 = colDef(name = "Floor Price", align = "left")
			)
		)
	})

	getPriceDistributionData <- reactive({
		id <- getTokenId()
		selected <- getCollection()
		tuple <- getConvertedPrice()
		if( length(id) == 0 | selected == '' ) {
			return(data.table())
		}
		cur <- pred_price[ token_id == eval(as.numeric(id)) & collection == eval(selected) ]
		mu_0 <- cur$pred_price[1]
		sd <- cur$pred_sd[1]
		mu <- adjust_price(mu_0, tuple)
		sd <- sd * (mu / mu_0) * SD_SCALE

		mn <- as.integer(max(0, mu - (sd * SD_MULT)))
		mx <- as.integer(mu + (sd * SD_MULT))
		r <- (mx - mn) / 100

		plot_data <- data.table()

		if( is.na(mu) ) {
			return(plot_data)
		}

		for (i in c(.2, .4, .6, .8)) {
			x <- ceiling(100*qnorm(i, mean = mu, sd = sd)) / 100
			# if (mu >= 100) {
			# 	x <- ceiling(qnorm(i, mean = mu, sd = sd))
			# }
			y <- pnorm(x, mu, sd)
			cur <- data.table(x = x, y = y )
			plot_data <- rbind( plot_data, cur )

			if (mu >= 100) {
				x <- x - 0.01
			}
			else {
				x <- x - 0.01
			}
			y <- pnorm(x, mu, sd)
			cur <- data.table(x = x, y = y )
			plot_data <- rbind( plot_data, cur )
		}

		if (mx - mn > 100) {
			for (x in seq(mn:mx)) {
				y <- pnorm(x, mu, sd)
				cur <- data.table(x = x, y = y )
				plot_data <- rbind( plot_data, cur )
			}
		} else if (mx - mn > 10){
			for (x in seq(mn, mx, .1)) {
				y <- pnorm(x, mu, sd)
				cur <- data.table(x = x, y = y )
				plot_data <- rbind( plot_data, cur )
			}
		} else {
			for (x in seq(mn, mx, .01)) {
				y <- pnorm(x, mu, sd)
				cur <- data.table(x = x, y = y )
				plot_data <- rbind( plot_data, cur )
			}
		}

		plot_data <- unique(plot_data)
		plot_data <- plot_data[order(x)]

		plot_data[, deal_score := round(100 * (1 - y))]
		# plot_data[, deal_score := ((mu - x) * 50 / (SD_MULT * eval(sd))) + 50  ]
		plot_data[, deal_score := round(pmin( 100, pmax(0, deal_score) ))  ]
		plot_data[, deal := ifelse(
			y < 0.2, 'Great Deal'
			, ifelse(
				y < 0.4, 'Good Deal'
				, ifelse(
					y < 0.6, 'Okay Deal'
					, ifelse(
						y < 0.8, 'Moderate Price'
						, 'Premium Price'
					)
				)
			)
		)]
		plot_data[, fillcolor := ifelse(
			y < 0.2, '#92CF28'
			, ifelse(
				y < 0.4, '#B6EE56'
				, ifelse(
					y < 0.6, '#E3FF7F'
					, ifelse(
						y < 0.8, '#FFA33F'
						, '#F78914'
					)
				)
			)
		)]

		plot_data[, points_hover := paste0("<b>", format(x, big.mark=","), "</b><br>",deal,"<br>Deal Score: ",deal_score,"")]
		plot_data <- plot_data[ x > 0 & y >= 0.004 & y <= 0.996 ]
		return( plot_data )
	})

	output$pricedistributionplot <- renderPlotly({

		plot_data <- getPriceDistributionData()
		if( nrow(plot_data) == 0 ) {
			return(NULL)
		}

		fig <- plot_ly(
			data = plot_data,
			x = ~x,
			y = ~y,
			type = 'scatter',
			mode = 'lines',

			# type = 'scatter',
			# mode = 'markers'
			fill = 'tozeroy',
			fillcolor = ~fillcolor,
			alpha_stroke = 0.0,
			# hoveron = 'points+fills',
			text = ~points_hover,
			hoverinfo = 'text'
		)
		fig <- fig %>% layout(
			showlegend = FALSE
			, margin = list(
				# l = 200,
				# r = 50,
				# b = 100,
				# t = 100,
				pad = 5
			)
			, xaxis = list(
				title = "Price"
				, showgrid = FALSE
				# , font = list(family = "Inter")
				# , fixedrange = TRUE
				, color = 'white'
				, zeroline = TRUE
				, nticks = 6
			)
			, yaxis= list(
				title = "Deal Score",
				ticktext = list("100", "80", "60", "40", "20", "0"), 
				tickvals = list(0, .2, .4, .6, .8, 1),
				# showticklabels = FALSE
				color = 'white'
				# , visible = FALSE
				# , automargin = FALSE
				# , showgrid = FALSE
				, zeroline = TRUE
				# , dividerwidth = 0
				# , standoff = 0
			)
			, plot_bgcolor = plotly.style$plot_bgcolor
			, paper_bgcolor = plotly.style$paper_bgcolor
		) %>%
		plotly::config(displayModeBar = FALSE) %>%
		plotly::config(modeBarButtonsToRemove = c("zoomIn2d", "zoomOut2d"))

	})

	getListingData <- reactive({
		selected <- getCollection()
		if( selected == '' ) {
			return(data.table())
		}

		df <- merge(listings[ collection == eval(selected), list(token_id, price) ], pred_price[ collection == eval(selected), list(token_id, pred_price, pred_sd, rk, nft_rank) ], by = c('token_id'))
		df <- merge(df, tokens[collection == eval(selected), list(collection, token_id, image_url)], by = c('token_id') )
		tuple <- getConvertedPrice()
		floors <- getFloors()

		df[, pred_price_0 := pred_price ]
		df[, pred_price := pred_price + eval(tuple[1]) + ( eval(tuple[2]) * pred_price / eval(floors[1]) ) ]
		df[, pred_price := pmax( eval(floors[2]), pred_price) ]
		df[, deal_score := ((pred_price - price) * 50 / (SD_MULT * pred_sd)) + 50  ]
		df[, deal_score := round(pmin( 100, pmax(0, deal_score) ))  ]
		df[, deal_score := pnorm(price, pred_price, eval(SD_SCALE) * pred_sd * pred_price / pred_price_0), by = seq_len(nrow(df)) ]
		df[, deal_score := round(100 * (1 - deal_score)) ]
		# df[, pred_price := round(pred_price) ]
		df[, pred_price_str := paste0(format(round(pred_price, 1), digits=3, decimal.mark=".", big.mark=",")) ]

		df <- df[, list(image_url, token_id, price, pred_price, pred_price_str, deal_score, rk, nft_rank)]
		m <- dcast(attributes[collection == eval(selected)], collection + token_id ~ feature_name, value.var='feature_value')
		df <- merge(df, m, all.x=TRUE, by=c('token_id'))
		df[, collection := NULL]
		df <- df[order(-deal_score)]
		return(copy(df))
	})


	output$listingplot <- renderPlotly({
		req(input$tokenid)
		df <- getListingData()
		if( nrow(df) == 0 ) {
			return(NULL)
		}
		df <- df[ deal_score >= 10 ]
		df[, hover_text := paste0('<b>#',token_id,'</b><br>Listing Price: ',price,'<br>Fair Market Price: ',pred_price_str,'<br>Deal Score: ',deal_score) ]
		f <- min(df[price > 0]$price)

		df <- df[ !(is.na(price)) & !(is.na(pred_price)) ]
		df[, price := as.numeric(price)]
		df[, pred_price := as.numeric(pred_price)]

		fig <- plot_ly(
			source = "listingLink",
			data = df,
			x = ~price,
			key = ~token_id, 
			y = ~pred_price,
			text = ~hover_text,
			hoverinfo = 'text',
			type = 'scatter',
			mode = 'markers',
			marker = list(
				size = 10,
				color = ~deal_score,
				colorbar=list(
					title = list(
						text = 'Deal Score',
						font = list(color='white')
					),
					tickfont = list(
						color = 'white'
					)
				),
				colorscale = list(
					c(0, 'rgb(237, 41, 56)'), 
					list(1, 'rgb(0, 255, 127)')
				),
				cauto = F,
				cmin = 0,
				cmax = 100
			)
		)
		fig <- fig %>% add_trace(
			data = df,
			marker = NULL,
			hoverinfo = 'text',
			text = '',
			x = ~price,
			y = ~price,
			name = 'reference',
			type = 'scatter',
			line = list(color = 'rgba(255, 255, 255, .5)'),
			mode = 'lines'
		)

		fig <- fig %>% layout(
			showlegend = FALSE
			, xaxis = list(
				title = "Listed Price"
				, color = 'white'
				, nticks = 7
				, showgrid = FALSE
                , range=list(f*0.9, f*2.5)
			)
			, yaxis= list(
				title = "Fair Market Price"
				, color = 'white'
				, nticks = 7
				, showgrid = FALSE
                , range=list(f * 0.9, f*2.5)
			)
			, plot_bgcolor = plotly.style$plot_bgcolor
			, paper_bgcolor = plotly.style$paper_bgcolor
		) 
        # %>%
		# plotly::config(displayModeBar = FALSE)
        #  %>%
		# plotly::config(modeBarButtonsToRemove = c("zoomIn2d", "zoomOut2d"))
		# event_register(fig, 'plotly_click')
	})

	convertCollectionName <- function(x) {
		if (length(x) == 0 ) {
			return('')
		}
		if (x == 'Solana Monkey Business') x <- 'solana-monkey-business'
		if (x == 'Degen Apes') x <- 'degen-ape-academy'
		if (x == 'Pesky Penguins') x <- 'peskypenguinclub'
		x <- tolower(x)
		return(x)
	}

	output$listingurl <- renderUI({
		selected <- getCollection()
		chain <- getChain()
		if(chain != 'Solana') {
			return(NULL)
		}
		name <- convertCollectionName(selected)
		if (name == 'peskypenguinclub') name <- 'pesky-penguins'
		href <- paste0('https://solanafloor.com/nft/',name,'/listed')
		url <- span("*Listings from ", a("solanafloor.com", href=href))
		HTML(paste(url))
    })

	output$solanaimg <- renderUI({
		chain <- getChain()
		class = ''
		if(length(chain) == 0) {
			return(NULL)
		}
		if(chain != 'Solana') {
			class <- 'opacity50'
		}
		t <- tags$img(src = 'Solana.png', class = class)
		t
    })

	output$terraimg <- renderUI({
		chain <- getChain()
		class = ''
		if(length(chain) == 0) {
			return(NULL)
		}
		if(chain != 'Terra') {
			class <- 'opacity50'
		}
		t <- tags$img(src = 'Terra.png', class = class)
		t
    })

	output$ethereumimg <- renderUI({
		chain <- getChain()
		class = ''
		if(length(chain) == 0) {
			return(NULL)
		}
		if(chain != 'Ethereum') {
			class <- 'opacity50'
		}
		t <- tags$img(src = 'Ethereum.png', class = class)
		t
    })

	output$howrareisurl <- renderUI({
		id <- getTokenId()
		selected <- getCollection()
		chain <- getChain()
		if(length(id) == 0 | selected == '' | length(chain) == 0) {
			return(NULL)
		}
		if( chain != 'Solana' | length(id) == 0 | selected == '' ) {
			return(NULL)
		}
		if (selected == 'Thugbirdz') {
			id <- str_pad(id, 4, pad='0')
		}
		name <- convertCollectionName(selected)
		if (name == 'solana-monkey-business') name <- 'smb'
		if (name == 'degen-ape-academy') name <- 'degenapes'
		href <- paste0('https://howrare.is/',name,'/',id)
        cur_0 <- pred_price[collection == eval(selected) ]
        cur_1 <- cur_0[ token_id == eval(id) ]

		url <- span("*Rarity from ", a("howrare.is", href=href),paste0(" (rank #",format(cur_1$nft_rank[1], big.mark = ','),") used in the model"))
		HTML(paste(url))
    })

	output$randomearthurl <- renderUI({
		id <- getTokenId()
		selected <- getCollection()
		chain <- getChain()
		if(chain == '') {
			return(NULL)
		}
		if( chain != 'Terra' | length(id) == 0 | selected == '' ) {
			return(NULL)
		}
		href <- tokens[ (collection == eval(selected)) & (token_id == eval(id)) ]$market_url[1]
		url <- span("View on ", a("randomearth.io", href=href))
		HTML(paste(url))
    })

	# observe({
	# 	req(input$tokenid)
	# 	req(nrow(getListingData()))
	# 	ed <- event_data("plotly_click", source = "listingLink")
	# 	if(!is.null(ed$key[1])) {
	# 		if (ed$key[1] != input$tokenid) {
	# 			updateSelectizeInput(session, 'tokenid', server = TRUE, selected = ed$key[1])
	# 			# updateTextInput(session = session, inputId = "tokenid", value = ed$key[1])
	# 			shinyjs::runjs("window.scrollTo(0, 300)")
	# 		}
	# 	}
	# })

	output$listingtable <- renderReactable({
		df <- copy(getListingData())
		if( nrow(df) == 0 ) {
			return(NULL)
		}
		# df[, rk:=paste0('#', format(rk, trim=TRUE, big.mark=","))]
		mx <- as.numeric(input$maxprice)
		if(!is.na(mx)) {
			df <- df[ price <= eval(mx) ]
		}
		mx <- as.numeric(input$maxnftrank)
		if(!is.na(mx)) {
			df <- df[ rk <= eval(mx) ]
		}
		mx <- as.numeric(input$maxrarityrank)
		if(!is.na(mx)) {
			df <- df[ nft_rank <= eval(mx) ]
		}
		df[, price := round(price, 2)]
		# df[, pred_price_str := paste0(format(round(pred_price, 1), digits=3, decimal.mark=".", big.mark=",")) ]
		df[, pred_price := NULL ]

		reactable(df,
			defaultColDef = colDef(
				headerStyle = list(background = "#10151A")
			),
			borderless = TRUE,
			filterable = TRUE,
			outlined = FALSE,
			columns = list(
                image_url = colDef(name = "Token", align = "left", cell = function(value, index) {
                    if(index <= 100) {
                        htmltools::tags$img(src=value)
                    } else {
                        return(NULL)
                    }
                }),
				token_id = colDef(name = "Token ID", align = "left"),
				price = colDef(name = "Listed Price", align = "left"),
				pred_price_str = colDef(name = "Fair Market Price", align = "left"),
				deal_score = colDef(name = "Deal Score", align = "left"),
				rk = colDef(name = "Deal Score Rank", align = "left"),
				nft_rank = colDef(name = "Rarity Rank", align = "left")
			),
			searchable = FALSE
	    )
	})


}
