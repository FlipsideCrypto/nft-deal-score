server <- function(input, output, session) {
	# load
	# list of token_ids
	# price distribution and average

	# select the token id
	# input the price
	# output the deal score
	load('data.Rdata')

	output$collectionselect <- renderUI({
		selectInput(
			inputId = 'collectionname'
			, label = NULL
			, selected = pred_price$collection[1]
			, choices = unique(pred_price$collection)
			, width = "100%"
		)
	})

	output$nftselect <- renderUI({
		selectInput(
			inputId = 'tokenid'
			, label = NULL
			, selected = pred_price$token_id[1]
			, choices = pred_price$token_id
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
		if (!is.na(id)) {
			t <- paste0("Hashmask #", id)
		}
		print("output$tokenid")
		print(paste0(id, t))
		paste0(t)
	})

	output$tokenrank <- renderText({
		id <- getTokenId()
		selected <- getCollection()
		t <- ""
		if (!is.na(id) & !is.na(collection)) {
			cur <- pred_price[ token_id == eval(as.numeric(input$tokenid)) & collection == eval(selected) ]
			if (nrow(cur)) {
				t <- paste0("Rank #", format(cur$rk[1], big.mark=",")," / ",format(nrow(pred_price), big.mark=","))
			}
		}
		print("output$tokenid")
		print(paste0(id, t))
		paste0(t)
	})

	output$fairmarketprice <- renderText({
		id <- getTokenId()
		selected <- getCollection()
		t <- ""
		if (!is.na(id)) {
			cur <- pred_price[ token_id == eval(as.numeric(input$tokenid)) & collection == eval(selected) ]
			if (nrow(cur)) {
				t <- paste0("Fair Market Price: $", (format(cur$pred_price, digits=3, decimal.mark=".",big.mark=",")))
			}
		}
		paste0(t)
	})

	output$priceinput <- renderUI({
		textInput(
			inputId = 'price'
			, label = NULL
			# , value = ""
			, width = "100%"
			, placeholder = NULL
		)
	})

	output$tokenimg <- renderUI({
		token_id <- getTokenId()
		collection <- getCollection()
		t <- tags$img(src = paste0("img/",token_id,".png"))
		if (TRUE | collection == 'Hashmasks') {
			t <- tags$img(src = paste0("img/",token_id,".png"))
		}
		t
	})

	getAttributesTable <- reactive({
		id <- getTokenId()
		selected <- getCollection()
		cur <- attributes[ token_id == eval(as.numeric(id)) & collection == eval(selected) ]
		cur <- merge( cur, feature_values[, list(feature, value, pct_vs_baseline) ] )
		return(cur)
	})

	output$attributestable <- renderReactable({
		data <- getAttributesTable()
		reactable(data[, list( feature, value, rarity, pct_vs_baseline )],
			defaultColDef = colDef(
				#align = "center",
				headerStyle = list(background = "#10151A")
			),
			borderless = TRUE,
			outlined = FALSE,
			columns = list(
				feature = colDef(name = "Attribute", align = "left"),
				value = colDef(name = "Value", align = "left"),
				rarity = colDef(name = "Rarity", align = "left", cell = function(x) {
					htmltools::tags$span(paste0(round(x*100, 1),'%'))
				}),
				pct_vs_baseline = colDef(name = "$ Value", align = "left", cell = function(x) {
					htmltools::tags$span(paste0('+', format(x*100, digits=3, decimal.mark=".", big.mark=","), '%'))
				})
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
		data <- pred_price[ collection == eval(selected), list( token_id, rk, pred_price, character, eyecolor, item, mask, skincolor )]
		data[, pred_price := paste0('$',format(pred_price, digits=3, decimal.mark=".", big.mark=",")) ]
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
				pred_price = colDef(name = "Est. Price", align = "left"),
				character = colDef(name = "Character", align = "left"),
				eyecolor = colDef(name = "Eye Color", align = "left"),
				item = colDef(name = "Item", align = "left"),
				mask = colDef(name = "Mask", align = "left"),
				skincolor = colDef(name = "Skin Color", align = "left")
			)
	    )
	})

	output$salestable <- renderReactable({
		selected <- getCollection()
		data <- sales[ collection == eval(selected) , list( token_id, block_timestamp, price_usd, pred, character, eyecolor, item, mask, skincolor )]
		print(paste0('salestable collection = ', collection))
		print(unique(data$collection))
		data[, price_usd := paste0('$',format(price_usd, digits=3, decimal.mark=".", big.mark=","))]
		data[, pred := paste0('$',format(pred, digits=3, decimal.mark=".", big.mark=","))]
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
				price_usd = colDef(name = "Price", align = "left"),
				pred = colDef(name = "Fair Market Price", align = "left"),
				character = colDef(name = "Character", align = "left"),
				eyecolor = colDef(name = "Eye Color", align = "left"),
				item = colDef(name = "Item", align = "left"),
				mask = colDef(name = "Mask", align = "left"),
				skincolor = colDef(name = "Skin Color", align = "left")
			)
	    )
	})

	getPriceDistributionData <- reactive({
		id <- getTokenId()
		selected <- getCollection()
		cur <- pred_price[ token_id == eval(as.numeric(id)) & collection == eval(selected) ]
		mu <- cur$pred_price[1]
		sd <- cur$pred_sd[1]

		mn <- as.integer(max(0, mu - (sd * 3)))
		mx <- as.integer(mu + (sd * 3))
		r <- (mx - mn) / 100

		plot_data <- data.table()

		for (i in c(.2, .4, .6, .8)) {
			x <- ceiling(qnorm(i, mean = mu, sd = sd))
			y <- pnorm(x, mu, sd)
			cur <- data.table(x = x, y = y )
			plot_data <- rbind( plot_data, cur )

			x <- x - 1
			y <- pnorm(x, mu, sd)
			cur <- data.table(x = x, y = y )
			plot_data <- rbind( plot_data, cur )
		}

		for (i in seq(1:100)) {
			x <- as.integer(mn+((i-1) * r))
			y <- pnorm(x, mu, sd)
			cur <- data.table(x = x, y = y )
			plot_data <- rbind( plot_data, cur )
		}

		plot_data <- plot_data[order(x)]

		plot_data[, deal_score := round((1 - y))]
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

		plot_data[, points_hover := paste0("<b>$", format(x, big.mark=","), "</b><br>",deal,"")]
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


}
