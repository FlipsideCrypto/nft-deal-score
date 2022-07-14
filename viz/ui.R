fluidPage(
	title = "NFT Deal Score",
    useShinyjs(),
	tags$head(
		tags$link(rel = "stylesheet", type = "text/css", href = "styles.css"),
		tags$link(rel = "icon", href = "fliptrans.png"),
	    tags$link(rel = "stylesheet", href = "https://fonts.googleapis.com/css?family=Roboto+Mono"),
	    tags$link(rel = "stylesheet", href = "https://fonts.googleapis.com/css?family=Inter")
	),
	tags$head(tags$script(src = "rudderstack.js")),
	tags$style(type="text/css",
		".shiny-output-error { visibility: hidden; }",
		".shiny-output-error:before { visibility: hidden; }"
	),
	withTags({
		header(class="top-banner",
		  section(
		    a(class="fs-logo", href="https://www.flipsidecrypto.com", "Powered by Flipside Crypto", onclick = "rudderstack.track('nft-click-flipside-icon')"),
		    section(class="socials",
		      a(class="twitter", href="https://twitter.com/flipsidecrypto", "Twitter", onclick = "rudderstack.track('nft-click-twitter-icon')"),
		      a(class="linkedin", href="https://www.linkedin.com/company/flipside-crypto", "LinkedIn", onclick = "rudderstack.track('nft-click-linkedin-icon')"),
		      a(class="discord", href="https://flipsidecrypto.com/discord", "Discord", onclick = "rudderstack.track('nft-click-discord-icon')"),
		      a(href="https://app.flipsidecrypto.com/auth/signup/velocity", "Sign Up", onclick = "rudderstack.track('nft-click-signup-icon')")
		    )
		  )
		)
	}),
	withTags({
		section(
			class="hero"
			, fluidRow(
				class = "header-images",
				column(6, uiOutput("solanaimg")),
				# column(4, uiOutput("terraimg")),
				column(6, uiOutput("ethereumimg"))
			)
			, h1(
				class="header", 
				"NFT Deal Score", 
				span(id="beta-text-tooltip", class="beta", "beta")
				, bsTooltip(id = "beta-text-tooltip", title = "This is a pilot app with 4 collections; plan is to expand to more collections and more chains!", placement = "right", trigger = "hover")
			),
			p(class='margin-bottom-0', "Check if an NFT listing is a deal, a steal, or a rip-off"),
			div("(not financial advice)"),
		)
	})
	, fluidRow(
		column(4
			, div(class = "inputtitle", "Select a Collection")
			, fluidRow(uiOutput("collectionselect"))
		)
		, column(4
			, div(
				class = "inputtitle"
				, "Select a Token ID"
				, icon(id="select-token-tooltip", "info-circle")
				, bsTooltip(id = "select-token-tooltip", title = "To search for a token, delete and then type in the desired token id", placement = "bottom", trigger = "hover")
			)
			# , fluidRow(uiOutput("nftselect"))
			# , fluidRow(selectInput("nftselect", "name", multiple = FALSE, choices = c('1','2','3','4','5','6','7','8','9','10')))
			, fluidRow(selectizeInput("tokenid", NULL, multiple = FALSE, choices = seq(1:100), selected=1))
			# , fluidRow(selectizeInput("listid", "A long list", choices = NULL))
		)
		, column(4
			, div(
				class = "inputtitle"
				, "Floor Price"
				, icon(id="floor-price-tooltip", "info-circle")
				, bsTooltip(id = "floor-price-tooltip", title = "Update this number to the current floor price of the collection, which will update the rest of the numbers on this page", placement = "bottom", trigger = "hover")
			)
			, fluidRow(uiOutput("floorpriceinput"))
		)
	)
	, fluidRow(
		class="grey8row"
		, fluidRow(
			div(
				class = "title"
				, textOutput("tokenid")
				, div(
					div(class = "subtitle", textOutput("tokenrank", inline=TRUE), icon(class="padding-left-5", id="rank-tooltip", "info-circle") )
					, bsTooltip(id = "rank-tooltip", title = "Dynamic value rank based on the estimated fair market price modeled from historical sales. Model and rank will update periodically as we get more sales data.", placement = "bottom", trigger = "hover")
				)
				, div(
					div(class = "subtitle", textOutput("rarityrank", inline=TRUE))
				)
				, div(class = "link", uiOutput('randomearthurl'))
			)
			, fluidRow(
				column(5
					, div(class = "token-img", uiOutput("tokenimg"))
				)
				, column(7, div(
					class = "table"
					, reactableOutput("attributestable")
					, bsTooltip(id = "value-tooltip", title = "Represents the dollar impact this feature has on the price vs the floor", placement = "bottom", trigger = "hover")
					)
				)
			)
			, div(
				class = 'light-container'
				, div(class = "title", textOutput("fairmarketprice"))
				, plotlyOutput("pricedistributionplot", height = 280)
			)
			, div(class = "link", uiOutput('howrareisurl'))
			# , div(
			# 	class = 'light-container'
			# 	, div(class = "title", textOutput("loanscore"))
			# 	, fluidRow(
			# 		column(4
			# 			, div(
			# 				class = "inputtitle"
			# 				, "Days"
			# 				, icon(id="loan-days-tooltip", "info-circle")
			# 				, bsTooltip(id = "loan-days-tooltip", title = "Update this number with the duration of the loan", placement = "bottom", trigger = "hover")
			# 			)
			# 			, fluidRow(uiOutput("loandaysinput"))
			# 		)
			# 		, column(4
			# 			, div(
			# 				class = "inputtitle"
			# 				, "Lending Amount"
			# 				, icon(id="loan-amount-tooltip", "info-circle")
			# 				, bsTooltip(id = "loan-amount-tooltip", title = "Update this number with the lending amount of the loan", placement = "bottom", trigger = "hover")
			# 			)
			# 			, fluidRow(uiOutput("loanamountinput"))
			# 		)
			# 		, column(4
			# 			, div(
			# 				class = "inputtitle"
			# 				, "Return Amount"
			# 				, icon(id="loan-return-tooltip", "info-circle")
			# 				, bsTooltip(id = "loan-return-tooltip", title = "Update this number with the return amount of the loan", placement = "bottom", trigger = "hover")
			# 			)
			# 			, fluidRow(uiOutput("loanreturninput"))
			# 		)
			# 	)
			# 	# , plotlyOutput("pricedistributionplot", height = 280)
			# )
		)
	)
	, fluidRow(
		class="grey8row"
		, h2("Listings", icon(class="padding-left-10", id="listings-tooltip", "info-circle"))
		# , bsTooltip(id = "listings-tooltip", title = "Plot only shows listings with deal score > 5; Click a dot to select the token", placement = "bottom", trigger = "hover")
		, bsTooltip(id = "listings-tooltip", title = "Plot only shows listings with deal score > 5", placement = "bottom", trigger = "hover")
		, div(
			class = "listing-plot"
			, plotlyOutput("listingplot", height = 500)
			, div(class='description', 'Plot only shows listings with deal score > 5')
			# , div(class='description', 'Click a dot to select the token')
		)
		, fluidRow(
            class = 'filters'
			, column(4
				, div(
					class = "inputtitle"
					, "Max Price"
				)
				, fluidRow(uiOutput("maxpriceinput"))
			)
			, column(4
				, div(
					class = "inputtitle"
					, "Max NFT Rank"
				)
				, fluidRow(uiOutput("maxnftrankinput"))
			)
			, column(4
				, div(
					class = "inputtitle"
					, "Max Rarity Rank"
				)
				, fluidRow(uiOutput("maxrarityrankinput"))
			)
		)
		, div(class = "table", reactableOutput("listingtable"))
		, div(class = "description", 'This app is still in beta - listings updates will be periodic (but at least 3x a week)')
		, div(class = "link", uiOutput('listingurl'))
	)
	, fluidRow(
		class="grey8row"
		, h2("Historical Sales", icon(class="padding-left-10", id="historical-sales-tooltip", "info-circle"))
		, bsTooltip(id = "historical-sales-tooltip", title = "This app is still in beta - sales data may be incomplete or delayed", placement = "bottom", trigger = "hover")
		, fluidRow(
            class = 'filters'
			, column(2
				, div(
					class = "inputtitle"
					, "Min Deal Score Rank"
				)
				, fluidRow(uiOutput("minnftrankinput2"))
			)
			, column(2
				, div(
					class = "inputtitle"
					, "Max Deal Score Rank"
				)
				, fluidRow(uiOutput("maxnftrankinput2"))
			)
			, column(2
				, div(
					class = "inputtitle"
					, "Min Rarity Rank"
				)
				, fluidRow(uiOutput("minrarityrankinput2"))
			)
			, column(2
				, div(
					class = "inputtitle"
					, "Max Rarity Rank"
				)
				, fluidRow(uiOutput("maxrarityrankinput2"))
			)
			, column(2
				, div(
					class = "inputtitle"
					, "Min Floor"
				)
				, fluidRow(uiOutput("minfloorinput"))
			)
			, column(2
				, div(
					class = "inputtitle"
					, "Max Floor"
				)
				, fluidRow(uiOutput("maxfloorinput"))
			)
			, column(3
				, div(
					class = "inputtitle"
					, textOutput('filter1', inline=TRUE)
				)
				, fluidRow(uiOutput("filter1select"))
			)
			, column(3
				, div(
					class = "inputtitle"
					, textOutput('filter2', inline=TRUE)
				)
				, fluidRow(uiOutput("filter2select"))
			)
			, column(3
				, div(
					class = "inputtitle"
					, textOutput('filter3', inline=TRUE)
				)
				, fluidRow(uiOutput("filter3select"))
			)
			, column(3
				, div(
					class = "inputtitle"
					, textOutput('filter4', inline=TRUE)
				)
				, fluidRow(uiOutput("filter4select"))
			)
			, column(3
				, div(
					class = "inputtitle"
					, textOutput('filter5', inline=TRUE)
				)
				, fluidRow(uiOutput("filter5select"))
			)
			, column(3
				, div(
					class = "inputtitle"
					, textOutput('filter6', inline=TRUE)
				)
				, fluidRow(uiOutput("filter6select"))
			)
			, column(3
				, div(
					class = "inputtitle"
					, textOutput('filter7', inline=TRUE)
				)
				, fluidRow(uiOutput("filter7select"))
			)
			, column(3
				, div(
					class = "inputtitle"
					, textOutput('filter8', inline=TRUE)
				)
				, fluidRow(uiOutput("filter8select"))
			)
			, column(3
				, div(
					class = "inputtitle"
					, textOutput('filter9', inline=TRUE)
				)
				, fluidRow(uiOutput("filter9select"))
			)
			, column(3
				, div(
					class = "inputtitle"
					, textOutput('filter10', inline=TRUE)
				)
				, fluidRow(uiOutput("filter10select"))
			)
			, column(3
				, div(
					class = "inputtitle"
					, textOutput('filter11', inline=TRUE)
				)
				, fluidRow(uiOutput("filter11select"))
			)
			, column(3
				, div(
					class = "inputtitle"
					, textOutput('filter12', inline=TRUE)
				)
				, fluidRow(uiOutput("filter12select"))
			)
		)
        , div(
            class = 'padding-bottom-1'
            , 'Average from most recent 100 sales using these filters: '
            , textOutput('salesAverage', inline=TRUE)
        )
		, div(class = "table", reactableOutput("salestable"))
		, div(class = "description", 'This app is still in beta - sales data may be incomplete or delayed')
	)
	, fluidRow(
		class="grey8row faq"
		, h2("FAQ")
		, h4("What is NFT Deal Score?")
		, div("We use historical sales data to determine the values and the rankings of each NFT.")
		, h4("Why is this rank different?")
		, div("Although rarity is a feature in our model, it is not just a rarity-based ranking. Certain features are put at a higher premium on the secondary marketplace, and this ranking reflects that.")
		, h4("Why are the rarity %s different?")
		, div("Our %s reflect only the NFTs in existence. Other tools may include more theoretical numbers.")
		, h4("How does the model work?")
		, div("Each attribute is an input into the model. We are working to add better model explanations to the tool.")
		, h4("How often is the data updated?")
		, div("Listings are updated 3x / week. Model is updated weekly.")
		, h4("Where can I send my questions?")
		, div(a(class="", href="https://twitter.com/nftdealscore", "@nftdealscore"), " on Twitter")
	)
	, fluidRow(
		class="grey8row"
		, h2("NFT Rankings", icon(class="padding-left-10", id="nft-rankings-tooltip", "info-circle"))
		, bsTooltip(id = "nft-rankings-tooltip", title = "Fair market price is based on training a machine learning model on historical sales data", placement = "bottom", trigger = "hover")
		, div(class = "table", reactableOutput("nftstable"))
		, div(class = "description", 'Fair market price is based on training a machine learning model on historical sales data')
	)
	# , fluidRow(
	# 	class="grey8row"
	# 	, h2("Feature Summary", icon(class="padding-left-10", id="feature-summary-tooltip", "info-circle"))
	# 	, bsTooltip(id = "feature-summary-tooltip", title = "Shows the rarity and estimated price impact of each feature", placement = "bottom", trigger = "hover")
	# 	, div(class = "table", reactableOutput("featurestable"))
	# 	, div(class = "description", 'Shows the rarity and estimated price impact of each feature')
	# )
)
