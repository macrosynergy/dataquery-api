const OAUTH_BASE_URL = "https://api-developer.jpmorgan.com/research/dataquery-authe/api/v2";
const TIMESERIES_ENDPOINT = "/expressions/time-series";
const HEARTBEAT_ENDPOINT = "/services/heartbeat";
const OAUTH_TOKEN_URL = "https://authe.jpmchase.com/as/token.oauth2";
const OAUTH_DQ_RESOURCE_ID = "JPMC:URI:RS-06785-DataQueryExternalApi-PROD";
const API_DELAY_PARAM = 0.3;
const EXPR_LIMIT = 20;

function requestWrapper(url, headers = null, params = null, method = "GET") {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open(method, url);
    xhr.setRequestHeader("Content-Type", "application/json");
    xhr.setRequestHeader("Accept", "application/json");
    xhr.setRequestHeader("Authorization", `Bearer ${headers.Authorization}`);

    xhr.onload = function () {
      if (xhr.status === 200) {
        resolve(xhr.response);
      } else {
        reject(xhr.response);
      }
    };
    xhr.onerror = function () {
      reject("Request failed.");
    };

    if (params) {
      xhr.send(JSON.stringify(params));
    } else {
      xhr.send();
    }
  });
}

class DQInterface {
  constructor(client_id, client_secret, proxy = null, dq_resource_id = OAUTH_DQ_RESOURCE_ID) {
    this.client_id = client_id;
    this.client_secret = client_secret;
    this.proxy = proxy;
    this.dq_resource_id = dq_resource_id;
    this.current_token = null;
    this.token_data = {
      grant_type: "client_credentials",
      client_id: this.client_id,
      client_secret: this.client_secret,
      aud: this.dq_resource_id,
    };
  }

  getAccessToken() {
    const isActive = (token) => {
      if (!token) {
        return false;
      } else {
        const created = new Date(token.created_at);
        const expires = token.expires_in;
        return (new Date() - created) / 60000 >= expires - 1;
      }
    };

    if (isActive(this.current_token)) {
      return this.current_token.access_token;
    } else {
      return new Promise((resolve, reject) => {
        requestWrapper(OAUTH_TOKEN_URL, null, this.token_data, "POST")
          .then((response) => {
            const r_json = JSON.parse(response);
            this.current_token = {
              access_token: r_json.access_token,
              created_at: new Date(),
              expires_in: r_json.expires_in,
            };
            resolve(this.current_token.access_token);
          })
          .catch((error) => {
            reject(error);
          });
      });
    }
  }

  request(url, params, method, headers = null) {
    return new Promise((resolve, reject) => {
      this.getAccessToken()
        .then((access_token) => {
          const full_url = OAUTH_BASE_URL + url;
          requestWrapper(full_url, { Authorization: access_token }, params, method)
            .then((response) => {
              resolve(JSON.parse(response));
            })
            .catch((error) => {
              reject(error);
            });
        })
        .catch((error) => {
          reject(error);
        });
    });
  }

  heartbeat() {
    // use the request function to make a heartbeat request
    response = new Promise((resolve, reject) => {
        this.request(HEARTBEAT_ENDPOINT, null, "GET")
            .then((response) => {
            resolve(response);
            })
            .catch((error) => {
            reject(error);
            });
    });
    // if the "info" in response dict, return true else return false
    return response["info"] == "OK";
    }



    download(
        expressions,
        start_date,
        end_date,
        calender = "CAL_ALLDAYS",
        frequency = "FREQ_DAY",
        conversion = "CONV_LASTBUS_ABS",
        nan_treatment = "NA_NOTHING",
    ) {
        
        // declare a dictionary to store predefined parameters
        let params_dict = {
            "format": "JSON",
            "start-date": start_date,
            "end-date": end_date,
            "calendar": calender,
            "frequency": frequency,
            "conversion": conversion,
            "nan_treatment": nan_treatment,
            "data": "NO_REFERENCE_DATA",
        }
        
        //  create a list of lists to store the expressions of batches = expr_limit. 
        let expr_list = []
        let expr_batch = []
        for (let i = 0; i < expressions.length; i++) {
            expr_batch.push(expressions[i])
            if (expr_batch.length == EXPR_LIMIT) {
                expr_list.push(expr_batch)
                expr_batch = []
            }
        }
        if (expr_batch.length > 0) {
            expr_list.push(expr_batch)
        }

        // assert that heartbeat is working
        if (!this.heartbeat()) {
            throw new Error("Heartbeat failed.")
        }
        
        // create a list to store the downloaded data
        let downloaded_data = []

        // loop through the batches of expressions
        for (let i = 0; i < expr_list.length; i++) {
            // create a copy of the params_dict
            let current_params = Object.assign({}, params_dict);
            // add the expressions to the copy of params_dict
            current_params["expressions"] = expr_list[i];
            // create a url
            let curr_url = OAUTH_BASE_URL + TIMESERIES_ENDPOINT;
            // create a list to store the current response
            let curr_response = {};
            // loop to get next page from the response if any
            let get_pagination = true;
            while (get_pagination) {
                // sleep(API_DELAY_PARAM)
                curr_response = this.request(curr_url, current_params, "GET");
                if (curr_response === null || !("instruments" in curr_response)) {
                    throw new Error("Invalid response.");
                } else {
                    downloaded_data = downloaded_data.concat(curr_response["instruments"]);
                    if ("links" in curr_response) {
                        if (curr_response["links"][1]["next"] === null) {
                            get_pagination = false;
                            break;
                        } else {
                            curr_url = OAUTH_BASE_URL + curr_response["links"][1]["next"];
                            current_params = {};
                        }
                    }
                }
            }
        }
        return downloaded_data;
    }

    to_array(downloaded_data) {
        /* for d in dict list 
        d["attributes"][0]["time-series"] has 2 values - first is datetime64[ns] and second is value of the expression

        create an output list of expression, date and value
        */
        let output = []
        for (let i = 0; i < downloaded_data.length; i++) {
            let d = downloaded_data[i];
            let expr = d["attributes"][0]["expression"];
            let date = d["attributes"][0]["time-series"][0];
            let value = d["attributes"][0]["time-series"][1];
            output.push([expr, date, value]);
        }
        return output;
    }
}

// create a main function to run the code
async function main() {
    let client_id = "<your_client_id>";
    let client_secret = "<your_client_secret>";

    // create an instance of the class
    let dqClient = new DQInterface(client_id, client_secret);

    // check heartbeat
    let heartbeat = await dqClient.heartbeat();
    console.log(heartbeat);
    
    // download data
    let expressions = [ "DB(JPMAQS,USD_EQXR_VT10,value)",
        "DB(JPMAQS,AUD_EXALLOPENNESS_NSA_1YMA,value)"];
    let start_date = "2020-01-01";
    let end_date = "2020-12-31";
    let downloaded_data = await dqClient.download(expressions, start_date, end_date);
    
    // convert the downloaded data to an array
    let output = dqClient.to_array(downloaded_data);

    console.log(output.slice(0, 10));
}