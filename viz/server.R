server <- function(input, output, session) {
	load('data.Rdata')

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
		if( length(selected) == 0 ) {
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

	output$collectionselect <- renderUI({
		choices <- sort(unique(pred_price$collection))
		selectInput(
			inputId = 'collectionname'
			, label = NULL
			, selected = 'Solana Monkey Business'
			, choices = choices
			, width = "100%"
		)
	})

	output$nftselect <- renderUI({
		selected <- getCollection()
		if( length(selected) == 0 ) {
			return(NULL)
		}
		choices <- sort(pred_price[collection == eval(selected)]$token_id)
		selectInput(
			inputId = 'tokenid'
			, label = NULL
			, choices = choices
			, width = "100%"
		)
	})
	getTokenId <- reactive({
		return(input$tokenid)
	})
	getCollection <- reactive({
		return(input$collectionname)
	})
	getChain <- reactive({
		selected <- getCollection()
		chain <- tokens[collection == eval(selected) ]$chain[1]
		return(chain)
	})

	output$tokenid <- renderText({
		id <- getTokenId()
		t <- ""
		selected <- getCollection()
		if( length(id) == 0 | length(selected) == 0 ) {
			return(t)
		}
		title <- ifelse(
			selected == 'Aurory'
			, 'Aurorian', ifelse(
				selected == 'Thugbirdz'
				, 'THUG'
				, ifelse(
					selected == 'Solana Monkey Business'
					, selected
					, substr(selected, 1, nchar(selected) - 1)
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
		if( length(id) == 0 | length(selected) == 0 ) {
			return(t)
		}
		if (!is.na(id) & !is.na(selected)) {
			cur_0 <- pred_price[collection == eval(selected) ]
			cur_1 <- cur_0[ token_id == eval(as.numeric(input$tokenid)) ]
			if (nrow(cur_1)) {
				t <- paste0("Rank #", format(cur_1$rk[1], big.mark=",")," / ",format(nrow(cur_0), big.mark=","))
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
		if( length(selected) == 0 ) {
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

	output$fairmarketprice <- renderText({
		id <- getTokenId()
		selected <- getCollection()

		t <- ""
		if( length(id) == 0 ) {
			return(t)
		}
		if ( !is.na(id) ) {
			cur <- pred_price[ token_id == eval(as.numeric(input$tokenid)) & collection == eval(selected) ]
			p_0 <- cur$pred_price[1]
			tuple <- getConvertedPrice()
			p_1 <- adjust_price(p_0, tuple)
			chain <- getChain()
			currency <- ifelse( chain == 'Solana', 'SOL', 'LUNA' )
			if (nrow(cur)) {
				t <- paste0("Fair Market Price: ", (format(p_1, digits=3, decimal.mark=".",big.mark=",")), " ", currency)
			}
		}
		paste0(t)
	})

	output$tokenimg <- renderUI({
		id <- getTokenId()
		selected <- getCollection()
		src <- tokens[ (collection == eval(selected)) & (token_id == eval(id)) ]$image_url[1]
		t <- tags$img(src = src)
		t
	})

	getAttributesTable <- reactive({
		id <- getTokenId()
		selected <- getCollection()
		if( length(id) == 0 | length(selected) == 0 ) {
			return(head(attributes, 0))
		}
		cur <- attributes[ token_id == eval(as.numeric(id)) & collection == eval(selected) ]
		# cur <- merge( cur, feature_values[collection == eval(selected), list(feature_name, feature_value, pred_vs_baseline, pct_vs_baseline) ], all.x=TRUE )
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
		cur[, vs_baseline := 0 ]
		cur[, pred_vs_baseline := 0 ]
		cur[, vs_baseline := 0 ]
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
		data <- data[, list( feature_name, feature_value, rarity )]
		reactable(data,
			defaultColDef = colDef(
				headerStyle = list(background = "#10151A")
			),
			defaultPageSize = 5,
			borderless = TRUE,
			outlined = FALSE,
			columns = list(
				feature_name = colDef(name = "Attribute", align = "left"),
				feature_value = colDef(name = "Value", align = "left"),
				rarity = colDef(name = "Rarity", align = "left")
				# pct_vs_baseline = colDef(
				# 	name="Value", header=with_tooltip("Value", "The estimated price impact of this feature vs the floor")
				# 	, html = TRUE
				# 	, align = "left"
				# 	, cell = function(x) {
				# 		htmltools::tags$span(paste0('+', format(round(x*1000)/10, digits=4, decimal.mark=".", big.mark=","), '%'))
				# 	}
				# )
			)
	    )
	})

	output$featurestable <- renderReactable({
		selected <- getCollection()
		if( length(selected) == 0 ) {
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
		data <- pred_price[ collection == eval(selected), list( token_id, rk, pred_price )]
		tuple <- getConvertedPrice()
		floors <- getFloors()
		data[, pred_price := pred_price + eval(tuple[1]) + ( eval(tuple[2]) * pred_price / eval(floors[1]) ) ]
		data[, pred_price := pmax( eval(floors[2]), pred_price) ]
		return(data)
	})

	output$nftstable <- renderReactable({
		selected <- getCollection()
		if( length(selected) == 0 ) {
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

	output$salestable <- renderReactable({
		selected <- getCollection()
		if( length(selected) == 0 ) {
			return(NULL)
		}
		data <- sales[ collection == eval(selected) , list( token_id, block_timestamp, price, pred )]
		data[, price := paste0(format(price, scientific = FALSE, digits=2, decimal.mark=".", big.mark=","))]
		data[, pred := paste0(format(round(pred, 1), scientific = FALSE, digits=2, decimal.mark=".", big.mark=","))]

		m <- pred_price[collection == eval(selected), list(token_id, rk)]
		data <- merge(data, m, all.x=TRUE)

		m <- dcast(attributes[collection == eval(selected), list(token_id, feature_name, clean_name)], token_id ~ feature_name, value.var='clean_name')
		data <- merge(data, m, all.x=TRUE)

		data <- data[order(-block_timestamp)]

		reactable(data,
			defaultColDef = colDef(
				headerStyle = list(background = "#10151A")
			),
			filterable = TRUE,
			borderless = TRUE,
			outlined = FALSE,
			searchable = FALSE,
			columns = list(
				token_id = colDef(name = "Token ID", align = "left"),
				block_timestamp = colDef(name = "Sale Date", align = "left"),
				price = colDef(name = "Price", align = "left"),
				pred = colDef(name = "Fair Market Price", align = "left"),
				rk = colDef(name = "Rank", align = "left")
			)
	    )
	})

	getPriceDistributionData <- reactive({
		id <- getTokenId()
		selected <- getCollection()
		tuple <- getConvertedPrice()
		if( length(id) == 0 | length(selected) == 0 ) {
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
		if( length(selected) == 0 ) {
			return(data.table())
		}

		df <- merge(listings[ collection == eval(selected), list(token_id, price) ], pred_price[ collection == eval(selected), list(token_id, pred_price, pred_sd, rk) ])
		df <- merge(df, tokens[collection == eval(selected), list(collection, token_id, image_url)] )
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
		df[, pred_price := paste0(format(round(pred_price, 1), digits=3, decimal.mark=".", big.mark=",")) ]

		df <- df[, list(image_url, token_id, price, pred_price, deal_score, rk)]
		m <- dcast(attributes[collection == eval(selected)], collection + token_id ~ feature_name, value.var='clean_name')
		df <- merge(df, m, all.x=TRUE)
		df[, collection := NULL]
		df <- df[order(-deal_score)]
		return(df)
	})


	output$listingplot <- renderPlotly({
		req(input$tokenid)
		df <- getListingData()
		if( nrow(df) == 0 ) {
			return(NULL)
		}
		df <- df[ deal_score >= 10 ]
		df[, hover_text := paste0('<b>#',token_id,'</b><br>Listing Price: ',price,'<br>Fair Market Price: ',pred_price,'<br>Deal Score: ',deal_score) ]
        f <- min(df$price)

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
		event_register(fig, 'plotly_click')
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
		if(chain == 'Terra') {
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
		if(chain != 'Solana') {
			class <- 'opacity50'
		}
		t <- tags$img(src = 'Solana.png', class = class)
		t
    })

	output$terraimg <- renderUI({
		chain <- getChain()
		class = ''
		if(chain != 'Terra') {
			class <- 'opacity50'
		}
		t <- tags$img(src = 'Terra.png', class = class)
		t
    })

	output$ethereumimg <- renderUI({
		chain <- getChain()
		class = ''
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
		if( chain != 'Solana' | length(id) == 0 | length(selected) == 0 ) {
			return(NULL)
		}
		if (selected == 'Thugbirdz') {
			id <- str_pad(id, 4, pad='0')
		}
		name <- convertCollectionName(selected)
		if (name == 'solana-monkey-business') name <- 'smb'
		if (name == 'degen-ape-academy') name <- 'degenapes'
		href <- paste0('https://howrare.is/',name,'/',id)
		url <- span("*Rarity from ", a("howrare.is", href=href)," used in the model")
		HTML(paste(url))
    })

	output$randomearthurl <- renderUI({
		id <- getTokenId()
		selected <- getCollection()
		chain <- getChain()
		if( chain != 'Terra' | length(id) == 0 | length(selected) == 0 ) {
			return(NULL)
		}
		href <- tokens[ (collection == eval(selected)) & (token_id == eval(id)) ]$market_url[1]
		url <- span("View on ", a("randomearth.io", href=href))
		HTML(paste(url))
    })

	observe({
		req(input$tokenid)
		ed <- event_data("plotly_click", source = "listingLink")
		if(!is.null(ed$key[1])) {
			updateTextInput(session = session, inputId = "tokenid", value = ed$key[1])
			shinyjs::runjs("window.scrollTo(0, 300)")
		}
	})

	output$listingtable <- renderReactable({
		df <- getListingData()
		if( nrow(df) == 0 ) {
			return(NULL)
		}
		# df[, rk:=paste0('#', format(rk, trim=TRUE, big.mark=","))]
		mx <- as.numeric(input$maxprice)
		if(!is.na(mx)) {
			df <- df[ price <= eval(mx) ]
		}
		mx <- as.numeric(input$maxnftrank)
		print('mx')
		print(mx)
		print(head(df$rk))
		if(!is.na(mx)) {
			df <- df[ rk <= eval(mx) ]
		}
		df[, price := round(price, 2)]

		reactable(df,
			defaultColDef = colDef(
				headerStyle = list(background = "#10151A")
			),
			borderless = TRUE,
			filterable = TRUE,
			outlined = FALSE,
			columns = list(
				image_url = colDef(name = "Token", align = "left", cell = function(value) {
                    htmltools::tags$img(src=value)
                }),
				token_id = colDef(name = "Token ID", align = "left"),
				price = colDef(name = "Listed Price", align = "left"),
				pred_price = colDef(name = "Fair Market Price", align = "left"),
				deal_score = colDef(name = "Deal Score", align = "left"),
				rk = colDef(name = "NFT Rank", align = "left")
			),
			searchable = FALSE
	    )
	})


}
