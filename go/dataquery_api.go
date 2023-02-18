package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// Constants. WARNING : DO NOT MODIFY.
const (
	oauthBaseURL       = "https://api-developer.jpmorgan.com/research/dataquery-authe/api/v2"
	timeSeriesEndpoint = "/expressions/time-series"
	heartbeatEndpoint  = "/services/heartbeat"
	oauthTokenURL      = "https://authe.jpmchase.com/as/token.oauth2"
	oauthDqResourceID  = "JPMC:URI:RS-06785-DataQueryExternalApi-PROD"
	apiDelayParam      = 0.3 // 300ms delay between requests.
	exprLimit          = 20  // Maximum number of expressions per request (not per "download").
)

// DQInterface is a wrapper around the JPMorgan DataQuery API.
type DQInterface struct {
	clientID     string
	clientSecret string
	proxy        string
	dqResourceID string
	currentToken *Token
	tokenData    url.Values
}

// Token represents the access token returned by the JPMorgan DataQuery API.
type Token struct {
	AccessToken string    `json:"access_token"`
	CreatedAt   time.Time `json:"created_at"`
	ExpiresIn   int       `json:"expires_in"`
}

// RequestWrapper is a wrapper function around the http.NewRequest() function.
func requestWrapper(url string, headers http.Header, params url.Values, method string) (*http.Response, error) {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header = headers
	req.URL.RawQuery = params.Encode()

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != 200 {
		return nil, errors.New(fmt.Sprintf("Request failed with status code %v.", res.StatusCode))
	}

	return res, nil
}

// NewDQInterface returns a new instance of the DQInterface type.
func NewDQInterface(clientID string, clientSecret string, proxy string, dqResourceID string) *DQInterface {
	return &DQInterface{
		clientID:     clientID,
		clientSecret: clientSecret,
		proxy:        proxy,
		dqResourceID: dqResourceID,
		currentToken: nil,
		tokenData: url.Values{
			"grant_type":    {"client_credentials"},
			"client_id":     {clientID},
			"client_secret": {clientSecret},
			"aud":           {dqResourceID},
		},
	}
}

// GetAccessToken returns an access token for the JPMorgan DataQuery API.
func (dq *DQInterface) GetAccessToken() string {
	if dq.currentToken != nil && dq.currentToken.isActive() {
		return dq.currentToken.AccessToken
	}

	res, err := requestWrapper(oauthTokenURL, http.Header{}, 
										dq.tokenData, "POST")
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()

	var token Token
	json.NewDecoder(res.Body).Decode(&token)

	token.CreatedAt = time.Now()
	dq.currentToken = &token

	return dq.currentToken.AccessToken
}

// IsActive returns true if the access token is active.
func (t *Token) isActive() bool {
	if t == nil {
		return false
	} else {
		return time.Since(t.CreatedAt).Seconds() < float64(t.ExpiresIn)
	}
}


// Heartbeat returns true if the heartbeat request is successful.
func (dq *DQInterface) Heartbeat() bool {
	res, err := requestWrapper(oauthBaseURL+heartbeatEndpoint, http.Header{}, url.Values{"data": {"NO_REFERENCE_DATA"}}, "GET")
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()

	var response map[string]interface{}
	json.NewDecoder(res.Body).Decode(&response)

	return response["info"] != nil
}

// Download returns a list of dictionaries containing the data for the given expressions.

func (dq *DQInterface) Download(expressions []string, format string, 
	start_date string, end_date string, calender string, frequency string, 
	conversion string, nan_treatment string) []map[string]interface{} {

		
	params := url.Values{
		"format":         {format},
		"start-date":     {start_date},
		"end-date":       {end_date},
		"calendar":       {calender},
		"frequency":      {frequency},
		"conversion":     {conversion},
		"nan_treatment":  {nan_treatment},
		"data":           {"NO_REFERENCE_DATA"}}
	
	
	if !dq.Heartbeat() {
		panic("Heartbeat failed.")
	}

	var data []map[string]interface{}
	
	for i := 0; i < len(expressions); i += exprLimit {
		end := i + exprLimit
		if end > len(expressions) {
			end = len(expressions)
		}

		exprSlice := expressions[i:end]
		// sleep to avoid hitting the rate limit.
	
		time.Sleep(time.Duration(apiDelayParam * 1000 * 1000 * 1000))
		
		// bool get_pagination = true
		var get_pagination bool = true
		for get_pagination {
			var curr_params url.Values = params
			curr_params["expressions"] = exprSlice
			var curr_url string = oauthBaseURL + timeSeriesEndpoint
			res, err := requestWrapper(curr_url, 
				http.Header{"Authorization": {"Bearer " + dq.GetAccessToken()}}, 
				curr_params, "GET")
				
			if err != nil {
				panic(err)
			}
			defer res.Body.Close()
			
			// if the response is nil, or it doesn't contain the "instruments" key, panic.
			var response map[string]interface{}
			json.NewDecoder(res.Body).Decode(&response)
			vx, ok := response["instruments"]
			if response == nil || !ok {
				panic("Invalid response from DataQuery API.")
			}
			
			// append 


			// if "links" in response, check if response["links"][1]["next"] is not nil

