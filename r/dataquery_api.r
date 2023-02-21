library(httr)
library(jsonlite)

# Constants. WARNING : DO NOT MODIFY.
OAUTH_BASE_URL <- "https://api-developer.jpmorgan.com/research/dataquery-authe/api/v2"
TIMESERIES_ENDPOINT <- "/expressions/time-series"
HEARTBEAT_ENDPOINT <- "/services/heartbeat"
OAUTH_TOKEN_URL <- "https://authe.jpmchase.com/as/token.oauth2"
OAUTH_DQ_RESOURCE_ID <- "JPMC:URI:RS-06785-DataQueryExternalApi-PROD"
API_DELAY_PARAM <- 0.3  # 300ms delay between requests.
EXPR_LIMIT <- 20  # Maximum number of expressions per request (not per "download").

request_wrapper <- function(url, headers=NULL, params=NULL, method="get", ...) {
  tryCatch({
    response <- httr::request(method=method, url=url, query=params, headers=headers, ...)
    if (response$status_code == 200) {
      return(response)
    } else {
      stop(paste0("Request failed with status code ", response$status_code, "."))
    }
  }, error=function(e) {
    if (inherits(e, "httr_error")) {
      if (e$message == "Could not resolve host: authe.jpmchase.com") {
        stop("Check your internet connection.")
      } else if (e$message == "Proxy Authentication Required") {
        stop("Proxy error. Check your proxy settings.")
      } else {
        stop(e$message)
      }
    } else {
      stop(e)
    }
  })
}

DQInterface <- function(client_id, client_secret, proxy=NULL, dq_resource_id=OAUTH_DQ_RESOURCE_ID) {
  dq_obj <- list(
    client_id=client_id,
    client_secret=client_secret,
    proxy=proxy,
    dq_resource_id=dq_resource_id,
    current_token=NULL,
    token_data=list(
      grant_type="client_credentials",
      client_id=client_id,
      client_secret=client_secret,
      aud=dq_resource_id
    )
  )
  
  get_access_token <- function() {
    is_active <- function(token=NULL) {
      if (is.null(token)) {
        return(FALSE)
      } else {
        created <- as.POSIXct(token$created_at)
        expires <- token$expires_in
        return(((Sys.time() - created) / 60) >= (expires - 1))
      }
    }
    
    if (is_active(dq_obj$current_token)) {
      return(dq_obj$current_token$access_token)
    } else {
      r_json <- request_wrapper(
        url=OAUTH_TOKEN_URL,
        data=dq_obj$token_data,
        method="POST",
        use_proxy=ifelse(!is.null(dq_obj$proxy), "use", "no"),
        proxy=dq_obj$proxy
      ) %>% content(as = "text") %>% fromJSON()
      dq_obj$current_token <- list(
        access_token=r_json$access_token,
        created_at=as.character(Sys.time()),
        expires_in=r_json$expires_in
      )
      return(dq_obj$current_token$access_token)
    }
  }
  
  requestx <- function(url, params, ...) {
    response <- request_wrapper(
      url=url,
      params=params,
      method="GET",
      use_proxy=ifelse(!is.null(dq_obj$proxy), "use", "no"),
      proxy=dq_obj$proxy,
      add_headers(.headers = c("Authorization" = paste("Bearer", get_access_token())))
    ) %>% content(as = "text") %>% fromJSON()
    return(response)
  }
  
  heartbeat <- function() {
    response <- requestx(
      url=paste0(OAUTH_BASE_URL, HEARTBEAT_ENDPOINT),
      params=list(data="NO_REFERENCE_DATA")
    )
    return("info" %in% names(response))
  }
  
}

# create a main function to call the DQInterface

