library(R6)
library(httr)
library(jsonlite)

# Constants. WARNING : DO NOT MODIFY.
OAUTH_BASE_URL <- "https://api-developer.jpmorgan.com/research/dataquery-authe/api/v2"
TIMESERIES_ENDPOINT <- "/expressions/time-series"
HEARTBEAT_ENDPOINT <- "/services/heartbeat"
OAUTH_TOKEN_URL <- "https://authe.jpmchase.com/as/token.oauth2"
OAUTH_DQ_RESOURCE_ID <- "JPMC:URI:RS-06785-DataQueryExternalApi-PROD"
API_DELAY_PARAM <- 0.3 # 300ms delay between requests.
EXPR_LIMIT <- 20 # Maximum number of expressions per request (not per "download").

request_wrapper <- function(url, headers = NULL, params = NULL, 
                                            method = "get", ...) {
  tryCatch(
    {
      response <- httr::request(method = method, url = url,
                                query = params, headers = headers, ...)
      if (response$status_code == 200) {
        return(response)
      } else {
        stop(paste0("Request failed with status code ", 
        response$status_code, "."))
      }
    },
    error = function(e) {
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
    }
  )
}



DQInterface <- R6Class("DQInterface",
  public = list(
    initialize = function(client_id, client_secret, proxy = NULL,
      dq_resource_id = OAUTH_DQ_RESOURCE_ID) {
      self$client_id <- client_id
      self$client_secret <- client_secret
      self$proxy <- proxy
      self$dq_resource_id <- dq_resource_id
    },
    heartbeat = function() {
      response <- request_wrapper(
        url = paste0(OAUTH_BASE_URL, HEARTBEAT_ENDPOINT),
        params = list(data = "NO_REFERENCE_DATA")
      )
      return("info" %in% names(response))
    }

  ),
  private = list(
    get_access_token = function() {
      is_active <- function(token) {
        if (is.null(token)) {
          return(FALSE)
        } else {
          created <- as.POSIXct(token$created_at, tz = "UTC")
          expires <- token$expires_in
          return((as.numeric(difftime(Sys.time(),
                  created, units = "mins")) / 60) >=
                  (expires - 1))
        }
      }

      if (is_active(self$current_token)) {
        return(self$current_token$access_token)
      } else {
        response <- request_wrapper(OAUTH_TOKEN_URL,
          body = list(
            grant_type = "client_credentials",
            client_id = self$client_id,
            client_secret = self$client_secret,
            aud = self$dq_resource_id
          ),
          encode = "form",
          config = add_proxy(self$proxy)
        )
        r_json <- jsonlite::fromJSON(httr::content(response, as = "text"))
        self$current_token <- list(
          access_token = r_json$access_token,
          created_at = as.character(Sys.time(), tz = "UTC"),
          expires_in = r_json$expires_in
        )
        return(self$current_token$access_token)
      }
    },

  requestx <- function(url, params, ...) {
    response <- request_wrapper(
      url = url,
      params = params,
      method = "GET",
      use_proxy = ifelse(!is.null(dq_obj$proxy), "use", "no"),
      proxy = dq_obj$proxy,
      add_headers(.headers = c("Authorization" = paste("Bearer", get_access_token())))
    ) %>%
      content(as = "text") %>%
      fromJSON()
    return(response)
  }
  )
)

# create a main function to call the DQInterface

client_id <- "<your_client_id>"
client_secret <- "<your_client_secret>"
dq_res_id <- OAUTH_DQ_RESOURCE_ID
proxy <- NULL
dq_obj <- DQInterface$new(client_id, client_secret, dq_res_id)


dq_obj$heartbeat()

