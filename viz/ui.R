fluidPage(
	title = "Flipside: NFT Deal Score App",
	tags$head(
		tags$link(rel = "stylesheet", type = "text/css", href = "styles.css"),
		tags$link(rel = "icon", href = "fliptrans.png"),
	    tags$link(rel = "stylesheet", href = "https://fonts.googleapis.com/css?family=Roboto+Mono"),
	    tags$link(rel = "stylesheet", href = "https://fonts.googleapis.com/css?family=Inter")
	),
	tags$style(type="text/css",
		".shiny-output-error { visibility: hidden; }",
		".shiny-output-error:before { visibility: hidden; }"
	),
	withTags({
		header(class="top-banner",
		  section(
		    a(class="fs-logo", href="https://www.flipsidecrypto.com", "Powered by Flipside Crypto"),
		    section(class="socials",
		      a(class="twitter", href="https://twitter.com/flipsidecrypto", "Twitter"),
		      a(class="linkedin", href="https://www.linkedin.com/company/flipside-crypto", "LinkedIn"),
		      a(class="discord", href="https://flipsidecrypto.com/discord", "Discord"),
		      a(href="https://app.flipsidecrypto.com/auth/signup/velocity", "Sign Up")
		    )
		  )
		)
	}),
	withTags({
		section(class="hero",
			# img(src = 'fliptrans.png', width = '100px'),
			# img(src = 'img/14555.png', width = '100px'),
			img(class = "barcode", src = 'barcode.png', width = '100px'),
			h1(class="thorchain-header", "NFT Deal Score App", span(class="beta", "beta")),
			p("Check if an NFT listing is a deal, a steal, or a rip-off"),
			# uiOutput("updatedat")
		)
	})
	, fluidRow(
		class="grey8row"
		, fluidRow(
			column(6
				, div(class = "inputtitle", "Select a Collection")
				, fluidRow(uiOutput("collectionselect"))
			)
			, column(6
				, div(class = "inputtitle", "Select an NFT")
				, fluidRow(uiOutput("nftselect"))
			)
		)
		, fluidRow(
			div(class = "title", textOutput("tokenid"), div(class = "subtitle", textOutput("tokenrank")))
			
			, fluidRow(
				column(4, div(class = "token-img", uiOutput("tokenimg")))
				, column(8, div(class = "table", reactableOutput("attributestable")))
			)
			, div(class = "title", textOutput("fairmarketprice"))
			, plotlyOutput("pricedistributionplot", height = 250)
		)
	)
	, fluidRow(
		class="grey8row"
		, h2("NFT Rankings")
		, div(class = "table", reactableOutput("nftstable"))
		# , div(class = "table", DT::dataTableOutput("nftstable"))
	)
	, fluidRow(
		class="grey8row"
		, h2("Historical Sales")
		, div(class = "table", reactableOutput("salestable"))
	)
	, fluidRow(
		class="grey8row"
		, h2("Feature Summary")
		, div(class = "table", reactableOutput("featurestable"))
	)
)
