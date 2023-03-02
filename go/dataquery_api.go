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
	// proxy        string
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
func NewDQInterface(clientID string, clientSecret string) *DQInterface {
	return &DQInterface{
		clientID:     clientID,
		clientSecret: clientSecret,
		currentToken: nil,
		tokenData: url.Values{
			"grant_type":    {"client_credentials"},
			"client_id":     {clientID},
			"client_secret": {clientSecret},
			"aud":           {oauthDqResourceID},
		},
	}
}

// GetAccessToken returns an access token for the JPMorgan DataQuery API.
func (dq *DQInterface) GetAccessToken() string {
	if dq.currentToken != nil && dq.currentToken.isActive() {
		return dq.currentToken.AccessToken
	} else {
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
	res, err := requestWrapper(oauthBaseURL+heartbeatEndpoint,
		http.Header{"Authorization": {"Bearer " + dq.GetAccessToken()}},
		url.Values{"data": {"NO_REFERENCE_DATA"}}, "GET")
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
	start_date string, end_date string) []map[string]interface{} {

	calender := "CAL_ALLDAYS"
	frequency := "FREQ_DAY"
	conversion := "CONV_LASTBUS_ABS"
	nan_treatment := "NA_NOTHING"
	ref_data := "NO_REFERENCE_DATA"
	params := url.Values{
		"format":        {format},
		"start-date":    {start_date},
		"end-date":      {end_date},
		"calendar":      {calender},
		"frequency":     {frequency},
		"conversion":    {conversion},
		"nan_treatment": {nan_treatment},
		"data":          {ref_data}}

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
			var response map[string]interface{}
			json.NewDecoder(res.Body).Decode(&response)
			v, ok := response["instruments"]
			if response == nil || !ok {
				panic("Invalid response from DataQuery API.")
			} else {
				data = append(data, v.([]map[string]interface{})...)

				if v, ok := response["links"]; ok {
					if v.([]interface{})[1].(map[string]interface{})["next"] != nil {
						curr_url = v.([]interface{})[1].(map[string]interface{})["next"].(string)
					} else {
						get_pagination = false
					}
				} else {
					get_pagination = false
				}
			}
		}
	}
	return data
}

// func (dq *DQInterface) extract_timeseries(downloaded_data []map[string]interface{}) []map[string]interface{} {
// 	// create a list of lists to store the timeseries data
// 	var timeseries_data []map[string]interface{}
// 	for i := 0; i < len(downloaded_data); i++ {
// 		// get dictionary for the current instrument
// 		d := downloaded_data[i].(map[[]]interface{})
// 		// get the expression for the current instrument
// 		expr := d["attributes"][0]["expression"]
// 		for j := 0; j < len(d["attributes"][0]["timeseries"].([]interface{})); j++ {
// 			// the first item in the list is the date. convert it from dateteime64[ns] to string
// 			date := d["attributes"][0]["timeseries"].([]interface{})[j].([]interface{})[0].(time.Time).Format("2006-01-02")
// 			// the second item in the list is the value. read it as a float64
// 			value := d["attributes"][0]["timeseries"].([]interface{})[j].([]interface{})[1].(float64)
// 			// create a dictionary to store the data
// 			var timeseries_dict map[string]interface{}
// 			timeseries_dict["date"] = date
// 			timeseries_dict["value"] = value
// 			timeseries_dict["expression"] = expr
// 			// append the dictionary to the list
// 			timeseries_data = append(timeseries_data, timeseries_dict)
// 		}
// 	}
// 	return timeseries_data
// }

// create a main function to test the code
func main() {

	client_id := "<your_client_id>"
	client_secret := "<your_client_secret>"

	// create a DQInterface object
	dq := NewDQInterface(client_id, client_secret)
	dq.Heartbeat()

}
