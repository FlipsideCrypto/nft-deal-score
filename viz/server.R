server <- function(input, output, session) {
	# load
	# list of token_ids
	# price distribution and average

	# select the token id
	# input the price
	# output the deal score
	load('data.Rdata')

	with_tooltip <- function(value, tooltip) {
		div(style = "text-decoration: underline; text-decoration-style: dotted; cursor: help",
		tippy(value, tooltip))
	}

	getFloors <- reactive({
		selected <- getCollection()
		floor_1 <- as.numeric(input$floorprice)
		floor_0 <- coefsdf[ collection == eval(selected) ]$floor_price[1]
		if (is.na(floor_1)) {
			floor_1 <- floor_0
		}
		floors <- c( floor_0, floor_1 )
		return(floors)
	})

	output$floorpriceinput <- renderUI({
		selected <- getCollection()
		textInput(
			inputId = 'floorprice'
			, label = NULL
			# , value = ""
			, width = "100%"
			, placeholder = coefsdf[ collection == eval(selected) ]$floor_price[1]
		)
	})

	output$collectionselect <- renderUI({
		selectInput(
			inputId = 'collectionname'
			, label = NULL
			# , selected = pred_price$collection[1]
			, selected = 'smb'
			, choices = unique(pred_price$collection)
			, width = "100%"
		)
	})

	output$nftselect <- renderUI({
		selected <- getCollection()
		selectInput(
			inputId = 'tokenid'
			, label = NULL
			, selected = NULL
			# , selected = '1'
			, choices = pred_price[collection == eval(selected)]$token_id
			, width = "100%"
		)
	})
	getTokenId <- reactive({
		return(input$tokenid)
	})
	getCollection <- reactive({
		return(input$collectionname)
	})

	output$tokenid <- renderText({
		id <- getTokenId()
		t <- ""
		selected <- getCollection()
		title <- ifelse(selected == 'smb', toupper(selected), toTitleCase(selected))
		if (!is.na(id)) {
			t <- paste0(title," #", id)
		}
		# print("output$tokenid")
		# print(paste0(id, t))
		paste0(t)
	})

	output$tokenrank <- renderText({
		id <- getTokenId()
		selected <- getCollection()
		t <- ""
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
		selected <- getCollection()
		t <- ""
		if (!is.na(id) & !is.na(selected)) {
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
		if (!is.na(id)) {
			cur <- pred_price[ token_id == eval(as.numeric(input$tokenid)) & collection == eval(selected) ]
			p_0 <- cur$pred_price[1]
			tuple <- getConvertedPrice()
			p_1 <- adjust_price(p_0, tuple)
			# p_1 <- (p_0 + tuple[1]) + (p_0 * tuple[2])
			# print(paste0("f_0=",f_0,". f_1=",f_1,". p_0=",p_0,". pct_chg=",pct_chg,". abs_chg=",abs_chg,". rat=",rat,". p_1=",p_1,""))
			if (nrow(cur)) {
				t <- paste0("Fair Market Price: ", (format(p_1, digits=3, decimal.mark=".",big.mark=",")))
			}
		}
		paste0(t)
	})

	output$tokenimg <- renderUI({
		token_id <- getTokenId()
		collection <- getCollection()
		t <- tags$img(src = paste0("img/",token_id,".png"))
		if (TRUE | collection == 'Hashmasks') {
			t <- tags$img(src = paste0("img/",collection,"/",token_id,".png"))
		}
		t
	})

	getAttributesTable <- reactive({
		id <- getTokenId()
		selected <- getCollection()
		cur <- attributes[ token_id == eval(as.numeric(id)) & collection == eval(selected) ]
		cur <- merge( cur, feature_values[collection == eval(selected), list(feature, value, pct_vs_baseline) ], all.x=TRUE )
		return(cur)
	})

	output$attributestable <- renderReactable({
		data <- getAttributesTable()
		data[, rarity := paste0(format(round(rarity*100, 2), digits=4, decimal.mark="."),'%') ]
		reactable(data[, list( feature, value, rarity, pct_vs_baseline )],
			defaultColDef = colDef(
				#align = "center",
				headerStyle = list(background = "#10151A")
			),
			defaultPageSize = 5,
			borderless = TRUE,
			outlined = FALSE,
			columns = list(
				feature = colDef(name = "Attribute", align = "left"),
				value = colDef(name = "Value", align = "left"),
				rarity = colDef(name = "Rarity", align = "left"),
				pct_vs_baseline = colDef(
					name="Value", header=with_tooltip("Value", "The estimated price impact of this feature vs the floor")
					, html = TRUE
					, align = "left"
					, cell = function(x) {
						htmltools::tags$span(paste0('+', format(round(x*100), digits=4, decimal.mark=".", big.mark=","), '%'))
					}
				)
			)
	    )
	})

	output$featurestable <- renderReactable({
		selected <- getCollection()
		data <- feature_values[ collection == eval(selected)]
		print(unique(data$collection))
		reactable(data[, list( feature, value, rarity, pct_vs_baseline )],
			defaultColDef = colDef(
				#align = "center",
				headerStyle = list(background = "#10151A")
			),
			borderless = TRUE,
			outlined = FALSE,
			searchable = TRUE,
			columns = list(
				feature = colDef(name = "Attribute", align = "left"),
				value = colDef(name = "Value", align = "left"),
				rarity = colDef(name = "Rarity", align = "left", cell = function(x) {
					htmltools::tags$span(paste0(format(x*100, digits=3, decimal.mark=".", big.mark=","),'%'))
				}),
				pct_vs_baseline = colDef(name = "$ Value", align = "left", cell = function(x) {
					htmltools::tags$span(paste0('+', format(x*100, digits=3, decimal.mark=".", big.mark=","), '%'))
				})
			)
	    )
	})

	output$nftstable <- renderReactable({
		selected <- getCollection()
		# data <- pred_price[ collection == eval(selected), list( token_id, rk, pred_price, attribute_count, type, clothes, ears, mouth, eyes, hat, background )]
		data <- pred_price[ collection == eval(selected), list( token_id, rk, pred_price )]
		# data[, pred_price := paste0(format(pred_price, digits=3, decimal.mark=".", big.mark=",")) ]
		tuple <- getConvertedPrice()
		floors <- getFloors()
		data[, pred_price := pred_price + eval(tuple[1]) + ( eval(tuple[2]) * pred_price / eval(floors[1]) ) ]
		data[, pred_price := pmax( eval(floors[2]), pred_price) ]
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
				# character = colDef(name = "Character", align = "left"),
				# eyecolor = colDef(name = "Eye Color", align = "left"),
				# item = colDef(name = "Item", align = "left"),
				# mask = colDef(name = "Mask", align = "left"),
				# skincolor = colDef(name = "Skin Color", align = "left")
			)
	    )
	})

	output$salestable <- renderReactable({
		selected <- getCollection()
		# data <- sales[ collection == eval(selected) , list( token_id, block_timestamp, price, pred, attribute_count, type, clothes, ears, mouth, eyes, hat, background )]
		data <- sales[ collection == eval(selected) , list( token_id, block_timestamp, price, pred )]
		print(paste0('salestable collection = ', selected))
		print(unique(data$collection))
		data[, price := paste0(format(price, scientific = FALSE, digits=2, decimal.mark=".", big.mark=","))]
		data[, pred := paste0(format(round(pred, 1), scientific = FALSE, digits=2, decimal.mark=".", big.mark=","))]
		reactable(data,
			defaultColDef = colDef(
				headerStyle = list(background = "#10151A")
			),
			borderless = TRUE,
			outlined = FALSE,
			searchable = TRUE,
			columns = list(
				token_id = colDef(name = "Token ID", align = "left"),
				block_timestamp = colDef(name = "Sale Date", align = "left"),
				price = colDef(name = "Price", align = "left"),
				pred = colDef(name = "Fair Market Price", align = "left")
				# character = colDef(name = "Character", align = "left"),
				# eyecolor = colDef(name = "Eye Color", align = "left"),
				# item = colDef(name = "Item", align = "left"),
				# mask = colDef(name = "Mask", align = "left"),
				# skincolor = colDef(name = "Skin Color", align = "left")
			)
	    )
	})

	getPriceDistributionData <- reactive({
		id <- getTokenId()
		selected <- getCollection()
		tuple <- getConvertedPrice()
		cur <- pred_price[ token_id == eval(as.numeric(id)) & collection == eval(selected) ]
		mu_0 <- cur$pred_price[1]
		sd <- cur$pred_sd[1]
		mu <- adjust_price(mu_0, tuple)
		sd <- sd * (mu / mu_0)

		mn <- as.integer(max(0, mu - (sd * 4)))
		mx <- as.integer(mu + (sd * 4))
		r <- (mx - mn) / 100

		plot_data <- data.table()

		for (i in c(.2, .4, .6, .8)) {
			x <- round(qnorm(i, mean = mu, sd = sd), 1)
			if (mu >= 25) {
				x <- ceiling(qnorm(i, mean = mu, sd = sd))
			}
			y <- pnorm(x, mu, sd)
			cur <- data.table(x = x, y = y )
			plot_data <- rbind( plot_data, cur )

			if (mu >= 25) {
				x <- x - 1
			}
			else {
				x <- x - 0.1
			}
			y <- pnorm(x, mu, sd)
			cur <- data.table(x = x, y = y )
			plot_data <- rbind( plot_data, cur )
		}

		for (i in seq(1:100)) {
			x <- round(mn+((i-1) * r), 1)
			if (mu >= 25) {
				x <- as.integer(x)
			}
			y <- pnorm(x, mu, sd)
			cur <- data.table(x = x, y = y )
			plot_data <- rbind( plot_data, cur )
		}

		plot_data <- plot_data[order(x)]

		plot_data[, deal_score := round(100 * (1 - y))]
		plot_data[, deal_score := ((mu - x) * 50 / (4 * eval(sd))) + 50  ]
		plot_data[, deal_score := round(pmin( 100, pmax(0, deal_score) ))  ]
		plot_data[, deal := ifelse(
			y < 0.2, 'Great Deal'
			, ifelse(
				y < 0.4, 'Good Deal'
				, ifelse(
					y < 0.6, 'Reasonable Deal'
					, ifelse(
						y < 0.8, 'Bad Deal'
						, 'Terrible Deal'
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

		plot_data[, points_hover := paste0("<b>$", format(x, big.mark=","), "</b><br>",deal,"<br>Deal Score: ",deal_score,"")]
		plot_data <- plot_data[ x > 0 ]
		return( plot_data )
	})

	output$pricedistributionplot <- renderPlotly({

		plot_data <- getPriceDistributionData()

		fig <- plot_ly(
			data = plot_data,
			x = ~x,
			y = ~y,
			type = 'scatter',
			mode = 'lines',
			fill = 'tozeroy',
			fillcolor = ~fillcolor,
			alpha_stroke = 0.0,
			hoveron = 'points+fills',
			# line = list(
			#   color = fillcolor
			# ),
			text = ~points_hover,
			hoverinfo = 'text'
		)
		fig <- fig %>% layout(
			showlegend = FALSE
			, xaxis = list(
				title = "Price"
				, showgrid = FALSE
				, tickprefix = "$"
				, font = list(family = "Inter")
				, fixedrange = TRUE
				, color = 'white'
			)
			, yaxis= list(
				showticklabels = FALSE
				, visible = FALSE
				, automargin = FALSE
				, showgrid = FALSE
				, zeroline = FALSE
				, dividerwidth = 0
				, standoff = 0
			)
			, plot_bgcolor = plotly.style$plot_bgcolor
			, paper_bgcolor = plotly.style$paper_bgcolor
		) %>%
		plotly::config(displayModeBar = FALSE) %>%
		plotly::config(modeBarButtonsToRemove = c("zoomIn2d", "zoomOut2d"))

	})

	getListingData <- reactive({
		selected <- getCollection()

		df <- merge(listings[ collection == eval(selected), list(token_id, price) ], pred_price[ collection == eval(selected), list(token_id, pred_price, pred_sd) ])
		tuple <- getConvertedPrice()
		floors <- getFloors()
		df[, pred_price := pred_price + eval(tuple[1]) + ( eval(tuple[2]) * pred_price / eval(floors[1]) ) ]
		df[, pred_price := pmax( eval(floors[2]), pred_price) ]
		# df[, pred_price := pred_price + eval(tuple[1]) + (pred_price * eval(tuple[2])) ]
		# df[, pred_price := pmax( floor, pred_price) ]
		# df[, pred_price := mapply( adjust_price, pred_price, eval(tuple))  ]
		df[, deal_score := ((pred_price - price) * 50 / (4 * pred_sd)) + 50  ]
		df[, deal_score := round(pmin( 100, pmax(0, deal_score) ))  ]
		df[, pred_price := round(pred_price) ]
		df <- df[, list(token_id, price, pred_price, deal_score)]
		df <- df[order(-deal_score)]
		return(df)
	})


	output$listingplot <- renderPlotly({
		df <- getListingData()
		df <- df[ deal_score >= 10 ]
		df[, hover_text := paste0('<b>#',token_id,'</b><br>Listing Price: ',price,'<br>Fair Market Price: ',pred_price,'<br>Deal Score: ',deal_score) ]

		fig <- plot_ly(
			source = "listingLink",
			data = df,
			x = ~price,
			key = ~token_id, 
			y = ~pred_price,
			text = ~hover_text,
			hoverinfo = 'text',
			type = 'scatter',
			marker = list(
				size = 10,
				# opacity = 0.5,
				# fillcolor = ~deal_score,
				color = ~deal_score,
				# color = 'rgba(255, 182, 193, .9)',
				# line = list(
				# 	color = ~deal_score,
				# 	width = 2,
				# 	opacity = 1.0
				# ),
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
					# c(0, 'rgb(255, 0, 0)'), 
					# list(1, 'rgb(0, 255, 0)')
				),
				cauto = F,
				cmin = 0,
				cmax = 100
				# reversescale = TRUE
			)
		) %>% onRender("
			function(el, x) {
				Plotly.d3.select('.cursor-crosshair').style('cursor', 'pointer')
			}
		")
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
			# legend = list(
			# 	title = "Deal Score",
			# 	font = list(
			# 		color = 'white'
			# 	)
			# )
			, xaxis = list(
				title = "Listed Price"
				# , showgrid = FALSE
				# , tickprefix = "$"
				# , font = list(family = "Inter")
				# , fixedrange = TRUE
				, color = 'white'
				# , rangemode = "tozero"
				# , type = "log"
			)
			, yaxis= list(
				title = "Fair Market Price"
				# , showticklabels = FALSE
				# , visible = FALSE
				# , automargin = FALSE
				# , showgrid = FALSE
				# , zeroline = FALSE
				# , dividerwidth = 0
				# , standoff = 0
				, color = 'white'
				# , rangemode = "tozero"
				# , type = "log"
			)
			, plot_bgcolor = plotly.style$plot_bgcolor
			, paper_bgcolor = plotly.style$paper_bgcolor
		) %>%
		plotly::config(displayModeBar = FALSE) %>%
		plotly::config(modeBarButtonsToRemove = c("zoomIn2d", "zoomOut2d"))
	})

	output$listingurl <- renderUI({
		selected <- getCollection()
		selected <- ifelse(
			selected == 'smb', 'solana-monkey-business', ifelse(
				selected == 'degenapes', 'degen-ape-academy', selected
			)
		)
		href <- paste0('https://solanafloor.com/nft/',selected,'/listed')
		url <- span("*Listings from ", a("solanafloor.com", href=href))
		HTML(paste(url))
    })

	output$howrareisurl <- renderUI({
		id <- getTokenId()
		selected <- getCollection()
		if (selected == 'thugbirdz') {
			id <- str_pad(id, 4, pad='0')
		}
		href <- paste0('https://howrare.is/',selected,'/',id)
		url <- span("*Rarity from ", a("howrare.is", href=href)," used in the model")
		HTML(paste(url))
    })

	observe({
		ed <- event_data("plotly_click", source = "listingLink")
		if(!is.null(ed$key[1])) {
			updateTextInput(session = session, inputId = "tokenid", value = ed$key[1])
			shinyjs::runjs("window.scrollTo(0, 300)")
		}
	})

	output$listingtable <- renderReactable({
		df <- getListingData()

		reactable(df,
			defaultColDef = colDef(
				headerStyle = list(background = "#10151A")
			),
			borderless = TRUE,
			outlined = FALSE,
			columns = list(
				token_id = colDef(name = "Token ID", align = "left"),
				price = colDef(name = "Listed Price", align = "left"),
				pred_price = colDef(name = "Fair Market Price", align = "left"),
				deal_score = colDef(name = "Deal Score", align = "left")
			)
	    )
	})


}
