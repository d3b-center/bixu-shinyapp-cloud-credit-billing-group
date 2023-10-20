# load packages
library(DBI)
library(RPostgres)
library(RPostgreSQL)
library(shiny)
library(shinyWidgets)
library(shinydashboard)
library(shinythemes)
library(config)
library(ggplot2)
library(rintrojs)
library(shinyBS)
library(DT)
library(dplyr)
library(plotly)
library(reshape2)
library(highcharter)
library(RColorBrewer)
library(data.table)
library(billboarder)
library(lubridate)
library(zoo)
library(readr)

## Connect to D3b data warehouse
# dw <- config::get()
# username <- dw$user
# password <- dw$pwd
# hostname <- "d3b-warehouse-aurora.cluster-cxxdzxepyea2.us-east-1.rds.amazonaws.com"
# database <- "postgres"

# con <- dbConnect(RPostgres::Postgres(),
#                  host = hostname,
#                  port = "5432",
#                  dbname = database,
#                  user = username,
#                  password = password)

# shinyapp UI
ui <- navbarPage(
    theme = shinytheme("yeti"),
    title = strong("Dashboard", style = "font-size:120%;color:white"),
    windowTitle = "cavatica-billinggroup",
    header = tagList(useShinydashboard()),
    inverse = TRUE,
    tabPanel(
      strong("Cloud Credit Billing Group Usage", style = "font-size:140%;"),
      fluidRow(
        column(
          width = 10, offset = 1,
          fluidRow(
            column(
              width = 4,
              br(),
              selectizeGroupUI(
                id = "filters",
                inline = FALSE,
                params = list(
                  billing_class = list(
                    inputId = "group",
                    title = a(icon("house"), HTML("&nbsp;"),
                    strong("Select Billing Group Class"), style = "font-size:150%"),
                    placeholder = "select"
                  ),
                  billing_group = list(
                    inputId = "billing_group",
                    title = a(icon("id-badge"), HTML("&nbsp;"),
                    strong("Select Billing Group Name"), style = "font-size:150%"),
                    placeholder = "select"
                  )
                )
              ),
              span(htmlOutput("text"), style = "color:red; font-size: 10px;"),
              style = "height:230px"
            ),
            column(
              width = 8,
              br(),
              br(),
              p("This is a shinyApp visualizes the CAVATICA billing group ",
                strong("current usage"),
                "stats. The data in this app was retrieved from ",
                a(href="https://docs.cavatica.org/reference/billing",
                strong("CAVATICA Billing API."), target = "_blank"),
                "It represents the \"current usage\" in terms of analysis and storage cost; it doesn't have or show previously invoiced data.",
                style = "text-align:justify;color:black;background-color:lavender;padding:30px;border-radius:20px"
              ),
              style = "height:180px"
            )
          ),
          fluidRow(
            column(
              width = 12,
              box(
                title = strong("Summary", style = "font-size:100%;"),
                status = "primary",
                width = "100%",
                solidHeader = TRUE,
                highchartOutput("barchart", height = "280px")
              )
            )
          ),
          fluidRow(
            column(
              width = 5,
              box(
                title = strong("Details", style = "font-size:100%;"),
                status = "primary",
                solidHeader = TRUE,
                width = "100%",
                dataTableOutput("table", height = "280px")
              )
            ),
            column(
              width = 7,
              box(
                  title = strong("Cost Summary", style = "font-size:100%;color:white"),
                  status = "primary",
                  width = "100%",
                  solidHeader = TRUE,
                  highchartOutput("plot", height = "280px")
              )
            )
          ),
          fluidRow(
            column(
              width = 6,
              box(
                  title = strong("Total Analysis Cost", style = "font-size:100%;color:white"),
                  status = "success",
                  width = "100%",
                  solidHeader = TRUE,
                  highchartOutput("plot1", height = "250px")
              )
            ),
            column(
                width = 6,
                box(
                    title = strong("Analysis Cost: Computation", style = "font-size:100%;color:white"),
                    status = "success",
                    width = "100%",
                    solidHeader = TRUE,
                    highchartOutput("plot2", height = "250px")
                )
            ),
            column(
                width = 6,
                box(
                    title = strong("Analysis Cost: Storage", style = "font-size:100%;color:white"),
                    status = "success",
                    width = "100%",
                    solidHeader = TRUE,
                    highchartOutput("plot3", height = "250px")
                )
            ),
            column(
              width = 6,
              box(
                  title = strong("Analysis Cost: Egress", style = "font-size:100%;color:white"),
                  status = "success",
                  width = "100%",
                  solidHeader = TRUE,
                  highchartOutput("plot4", height = "250px")
              )
            )
          )
        )
      ),

    )
)

# shinyapp Server
server <- shinyServer(function(input, output, session) {

## pull dataframe from dataearehouse
  # all_analysis <- "SELECT * from bix_reporting.cavatica_bg_analysis;"
  # all_storage <-  "SELECT * from bix_reporting.cavatica_bg_storage;"
  # all_balance <-  "SELECT * from bix_reporting.cavatica_bg_balance;"

  # analysis <- dbGetQuery(con, all_analysis)
  # storage <- dbGetQuery(con, all_storage)
  # balance <- dbGetQuery(con, all_balance)

  analysis <- read.table("/Users/xiaoyan/Documents/work/project/airflow_dev/dags/billing_group/cavatica_bg_analysis.txt", sep = '\t',header = TRUE,check.names = FALSE)
  storage <- read.table("/Users/xiaoyan/Documents/work/project/airflow_dev/dags/billing_group/cavatica_bg_storage.txt", sep = '\t',header = TRUE,check.names = FALSE)
  balance <- read.table("/Users/xiaoyan/Documents/work/project/airflow_dev/dags/billing_group/cavatica_bg_balance.txt", sep = '\t',header = TRUE,check.names = FALSE)

  storage_new <- storage[!duplicated(storage), ]
  analysis_new <- analysis[!duplicated(analysis), ]

  filter_data <- callModule(
    module = selectizeGroupServer,
    id = "filters",
    data = balance,
    inline = FALSE,
    vars = c("group","billing_group")
  )

  show_table <- reactive({
    billing_groups <- unique(sort(filter_data()$billing_group))
    analysis_tmp <- analysis_new %>%
      filter(billing_group %in% billing_groups) %>%
      group_by(billing_group) %>%
      summarise(Tasks_cost = sum(Tasks), Studio_cost = sum(Studio))

    storage_tmp <- storage_new %>%
      filter(billing_group %in% billing_groups) %>%
      group_by(billing_group) %>%
      summarise(Storage_cost = sum(price)) %>%
      select(billing_group, Storage_cost)

    balance_tmp <- balance %>%
      filter(billing_group %in% billing_groups)

    all_tmp <- left_join(analysis_tmp,storage_tmp, by = "billing_group") %>%
      left_join(., balance_tmp, by = "billing_group")
    all_tmp[is.na(all_tmp)] <- 0
    all <- all_tmp %>%
        rowwise() %>%
        mutate(Usage = sum(Tasks_cost, Studio_cost, Storage_cost))
    all <- as.data.frame(all)
    return(all)
  })

  output$text <- renderUI({
    req(show_table())
    req(filter_data())
    billing_groups <- unique(sort(filter_data()$billing_group))

    check_len <- nrow(show_table())
    if (check_len == 0) {
      text1 <- "********** NOTE **********"
      text2 <- paste0(billing_groups, " shows a balance of 0 and no task/storage/studio costs.")
      HTML(paste(text1, text2, sep = "<br/>"))
    }
  })

  output$table <- renderDataTable(server = FALSE, {
    tmp <- t(show_table())
    datatable(tmp, colnames = rep("", ncol(tmp)),
      options = list(dom = "t", ordering = F, scrollX = TRUE, scrollY = "250px"))
    })

  output$barchart <- renderHighchart({
    balance_table <- as.data.frame(show_table()) %>%
                    select(billing_group, Balance, Usage)
    new <- melt(balance_table, id = "billing_group")
    hchart(new, type = 'column', hcaes(x = billing_group, y = value, group = variable)) %>%
      hc_colors(c("#3478a9", "orange")) %>%
      hc_yAxis(title = list(text = "Price(USD)")) %>%
      hc_xAxis(title = list(text = ""))
  })

  # generate barchart
  ## Cost Summary
  output$plot <- renderHighchart({
    req(show_table())
    cols <- brewer.pal(3, "Set2")

    Tasks <- sum(show_table()$Tasks_cost)
    Studio <- sum(show_table()$Studio_cost)
    Storage <- sum(show_table()$Storage_cost)
    new <- cbind(Tasks, Studio, Storage)
    rownames(new) <- "cost"
    pre_data <- t(new)
    data <- as.data.frame(cbind(type = rownames(pre_data), pre_data))
    row.names(data) = NULL
    final_data <- transform(data, cost = as.numeric(cost))
    highchart() %>%
      hc_add_series(final_data, hcaes(x = type,  y = cost), type = 'pie', innerSize="50%", cex=5, size = "120%") %>%
      hc_tooltip(headerFormat = "<b>{point.name}</b>", pointFormat = "{point.name} <br> cost: {point.y:.2f}") %>%
      hc_colors(c("#3478a9", "lightgreen", "orange")) %>%
      hc_plotOptions(series = list(
        dataLabels =  list(format = "<b>{point.name}</b>:  {point.percentage:.2f} %")
      ))
  })

## Total Analysis Cost
  output$plot1 <- renderHighchart({
    req(filter_data())
    billing_groups <- unique(sort(filter_data()$billing_group))
    subtable <- analysis_new %>% filter(billing_group %in% billing_groups)

    tmp_manifest <- subtable %>%
      mutate(month = month(time_started),year = year(time_started))
    tmp <- tmp_manifest %>%
      group_by(billing_group, analysis_type, year, month) %>%
      summarise(Monthly_cost = sum(total_cost)) %>%
      mutate(date = as.Date(paste(year, month, 01, sep = "-"), format = "%Y-%m-%d")) %>% arrange(date)
    k <- nrow(tmp)
    data1 <- tmp[, "Monthly_cost"]
    data2 <- data1 %>%
      mutate(Accumulate_cost = rollapplyr(Monthly_cost, k, sum, partial = TRUE))
    data3 <- cbind(tmp, Accumulate_cost = data2$Accumulate_cost)
    data_task <- data3 %>% filter(analysis_type == "TASK")
    data_cruncher <- data3 %>% dplyr::filter(analysis_type != "TASK")

    highchart() %>%
      hc_add_series(data3, hcaes(x = date,  y = Accumulate_cost),
                    name = "Accumulated", color = "#E58606",
                    type = "line", lineWidth = 5, dashStyle = "ShortDot") %>%
      hc_add_series(data_task, hcaes(x= date,  y = Monthly_cost),
                    name = "Monthly Task", color = "#52BCA3",
                    type = "line", lineWidth = 5) %>%
      hc_add_series(data_cruncher, hcaes(x= date,  y = Monthly_cost),
                    name = "Monthly DataStudio",
                    type = "line", lineWidth = 5) %>%
      hc_xAxis(dateTimeLabelFormats = list(month = "%Y-%m"),
                type = "datetime", labels = list(fontSize = "100px")) %>%
      hc_yAxis(title = list(text = "Price(USD)")) %>%
      hc_tooltip(pointFormat = "Month: {point.x:%b} <br> Cost: {point.y}", xDateFormat = " ") %>%
      hc_plotOptions(line = list(marker = list(lineWidth = 1, radius = 10)))
  })

## Analysis: Computation Cost
  output$plot2 <- renderHighchart({
    req(filter_data())
    billing_groups <- unique(sort(filter_data()$billing_group))
    subtable <- analysis_new %>% filter(billing_group %in% billing_groups)

    tmp_manifest <- subtable %>%
      mutate(month = month(time_started),year = year(time_started))
    tmp <- tmp_manifest %>%
      group_by(billing_group, analysis_type,year,month) %>%
      summarise(Monthly_cost = sum(computation_cost)) %>%
      mutate(date = as.Date(paste(year, month, 01, sep = "-"), format = "%Y-%m-%d")) %>% arrange(date)

    k <- nrow(tmp)
    data1 <- tmp[, "Monthly_cost"]
    data2 <- data1 %>%
      mutate(Accumulate_cost = rollapplyr(Monthly_cost, k, sum, partial = TRUE))
    data3 <- cbind(tmp, Accumulate_cost = data2$Accumulate_cost)

    data_task <- data3 %>% filter(analysis_type == "TASK")
    data_cruncher <- data3 %>% dplyr::filter(analysis_type != "TASK")

    highchart() %>%
      hc_add_series(data3, hcaes(x = date,  y = Accumulate_cost),
                    name = "Accumulated", color = "#E58606",
                    type = "line", lineWidth = 5, dashStyle = "ShortDot") %>%
      hc_add_series(data_task, hcaes(x = date,  y = Monthly_cost),
                    name = "Monthly Task", color = "#52BCA3",
                    type = "line", lineWidth = 5) %>%
      hc_add_series(data_cruncher, hcaes(x = date,  y = Monthly_cost),
                    name = "Monthly DataStudio",
                    type = "line", lineWidth = 5) %>%
      hc_xAxis(dateTimeLabelFormats = list(month = "%Y-%m"),
              type = "datetime", labels = list(fontSize = "100px")) %>%
      hc_yAxis(title = list(text = "Price(USD)")) %>%
      hc_tooltip(pointFormat = "Month: {point.x:%b} <br> Cost: {point.y}", xDateFormat = " ") %>%
      hc_plotOptions(line = list(marker = list(lineWidth = 1, radius = 10)))

  })

## Analysis: Storage Cost
  output$plot3 <- renderHighchart({
    req(filter_data())
    billing_groups <- unique(sort(filter_data()$billing_group))
    subtable <- analysis_new %>% filter(billing_group %in% billing_groups)

    tmp_manifest <- subtable %>%
      mutate(month = month(time_started), year = year(time_started))
    tmp <- tmp_manifest %>%
      group_by(billing_group, analysis_type, year, month) %>%
      summarise(Monthly_cost = sum(storage_cost)) %>%
      mutate(date = as.Date(paste(year, month, 01, sep = "-"), format = "%Y-%m-%d")) %>%
      arrange(date)

    k <- nrow(tmp)
    data1 <- tmp[, "Monthly_cost"]
    data2 <- data1 %>%
      mutate(Accumulate_cost = rollapplyr(Monthly_cost, k, sum, partial = TRUE))
    data3 <- cbind(tmp, Accumulate_cost = data2$Accumulate_cost)

    data_task <- data3 %>% filter(analysis_type == "TASK")
    data_cruncher <- data3 %>% dplyr::filter(analysis_type != "TASK")

    highchart() %>%
      hc_add_series(data3, hcaes(x = date,  y = Accumulate_cost),
                    name = "Accumulated", color = "#E58606",
                    type = 'line', lineWidth = 5, dashStyle = "ShortDot") %>%
      hc_add_series(data_task, hcaes(x= date,  y = Monthly_cost),
                    name = "Monthly Task", color = "#52BCA3",
                    type = 'line', lineWidth = 5) %>%
      hc_add_series(data_cruncher, hcaes(x= date,  y = Monthly_cost),
                    name = "Monthly DataStudio", type = "line",
                    lineWidth = 5) %>%
      hc_xAxis(dateTimeLabelFormats = list(month = "%Y-%m"),
              type = "datetime", labels = list(fontSize = "100px")) %>%
      hc_yAxis(title = list(text = "Price(USD)")) %>%
      hc_tooltip(pointFormat = "Month: {point.x:%b} <br> Cost: {point.y}", xDateFormat = " ") %>%
      hc_plotOptions(line = list(marker = list(lineWidth = 1, radius = 10)))

  })

## Analysis: Egress Cost
  output$plot4 <- renderHighchart({
    req(filter_data())
    billing_groups <- unique(sort(filter_data()$billing_group))
    subtable <- analysis_new %>% filter(billing_group %in% billing_groups)

    tmp_manifest <- subtable %>%
      mutate(month = month(time_started), year = year(time_started))
    tmp <- tmp_manifest %>%
      group_by(billing_group,analysis_type, year,month) %>%
      summarise(Monthly_cost = sum(egress_cost)) %>%
      mutate(date = as.Date(paste(year, month, 01, sep = "-"), format = "%Y-%m-%d")) %>%
      arrange(date)

    k <- nrow(tmp)
    data1 <- tmp[, "Monthly_cost"]
    data2 <- data1 %>%
      mutate(Accumulate_cost = rollapplyr(Monthly_cost, k, sum, partial = TRUE))
    data3 <- cbind(tmp, Accumulate_cost = data2$Accumulate_cost)

    data_task <- data3 %>% filter(analysis_type == "TASK")
    data_cruncher <- data3 %>% dplyr::filter(analysis_type != "TASK")

    highchart() %>%
      hc_add_series(data3, hcaes(x = date,  y = Accumulate_cost),
                    name = "Accumulated", color = "#E58606",
                    type = 'line', lineWidth = 5, dashStyle = "ShortDot") %>%
      hc_add_series(data_task, hcaes(x= date,  y = Monthly_cost),
                    name = "Monthly Task", color = "#52BCA3",
                    type = 'line', lineWidth = 5) %>%
      hc_add_series(data_cruncher, hcaes(x= date,  y = Monthly_cost),
                    name = "Monthly DataStudio", type = "line",
                    lineWidth = 5) %>%
      hc_xAxis(dateTimeLabelFormats = list(month = "%Y-%m"),
              type = "datetime", labels = list(fontSize='100px')) %>%
      hc_yAxis(title = list(text = "Price(USD)")) %>%
      hc_tooltip(pointFormat = "Month: {point.x:%b} <br> Cost: {point.y}",xDateFormat = " ") %>%
      hc_plotOptions(line = list(marker = list(lineWidth = 1, radius = 10)))

  })


})
shinyApp(ui, server)
