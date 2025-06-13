import functions_framework
import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time
from google.cloud import bigquery


@functions_framework.http
def test(request):
#######################################################################################
## Figure out latest values
#######################################################################################

    def get_latest_date(rate_to_get):

        # Define the parameter
        job_config = bigquery.QueryJobConfig(
           query_parameters=[
                bigquery.ScalarQueryParameter("currency", "STRING", rate_to_get)
            ]
        )

        query = """SELECT max(from_date) as latest FROM `fx.exchangerates` WHERE currency = @currency"""
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()
 
        latest_value_string = str(next(results))
        date_part_string = latest_value_string[19:-19]
        if date_part_string == '':
            date_part_string = str("2021-01-01")
            date_format = '%Y-%m-%d'
            convert_date_part_string_to_date_object = datetime.strptime(date_part_string, date_format)
            from_date = convert_date_part_string_to_date_object
            return from_date.strftime('%Y-%m-%d')
        else:
            date_format = '%Y, %m, %d'
            convert_date_part_string_to_date_object = datetime.strptime(date_part_string, date_format) 
            first_date_of_month = convert_date_part_string_to_date_object.replace(day=1)
            plus_one_month_object = first_date_of_month + relativedelta(months=1)
            from_date = plus_one_month_object.strftime('%Y-%m-%d')    
            return str(from_date)
#######################################################################################
## Query Riksbanken
#######################################################################################
#
    exchangerates_to_get = ['CAD','AUD','EUR','GBP','MXN','NZD','SGD','USD','DKK','JPY','NOK']
    #Connect to BigQuery
    client = bigquery.Client()
    #Settings for project, schema and table
    table_id = "riksbanken.fx.exchangerates"
    #create empty array to be able to store the results of the different queries
    result = []
    for rate_to_get in exchangerates_to_get:
        seriesId = 'SEK' + rate_to_get + 'PMI'
        from_date = get_latest_date(str(rate_to_get))
        #Call Riksbanken
        response = requests.get('https://api.riksbank.se/swea/v1/ObservationAggregates/' + seriesId + '/M/' + from_date)
        #Handle the response
        if str(response) != "<Response [204]>":        
            response_json = response.json()
            #We are getting the first and last WORKINGday of the month but we want the first and last CALENDARday of the month so here we are adjusting this            
            new_json = []
            for value in range(0, len(response_json), 1):
                date_format = '%Y-%m-%d'
                convert_from_date_part_string_to_date_object = datetime.strptime(response_json[value]["from"], date_format) 
                first_date_of_month = convert_from_date_part_string_to_date_object.replace(day=1)
                from_date = first_date_of_month.strftime('%Y-%m-%d')

                convert_to_date_part_string_to_date_object = datetime.strptime(response_json[value]["to"], date_format) 

                next_month = convert_to_date_part_string_to_date_object.replace(day=28) + timedelta(days=4)
                to_date = (next_month - timedelta(days=next_month.day)).strftime('%Y-%m-%d')

                currency = rate_to_get
                average_value = response_json[value]["average"]
                min_value = response_json[value]["min"] 
                max_value = response_json[value]["max"] 
                ultimo_value = response_json[value]["ultimo"]

                json = {"currency": currency, "from": from_date, "to": to_date, "average": average_value, "min": min_value, "max": max_value, "ultimo": ultimo_value }
                new_json.append(json)
#            return str(new_json)

            if len(new_json) > 0:
                values = [{ "currency": new_json[value]["currency"],
                    "from_date": new_json[value]["from"], 
                    "to_date": new_json[value]["to"], 
                    "average_value": round(new_json[value]["average"], 4), 
                    "min_value": round(new_json[value]["min"], 4), 
                    "max_value": round(new_json[value]["max"], 4), 
                    "ultimo_value": round(new_json[value]["ultimo"], 4)} 
                for value in range(0, len(new_json), 1)]
#                return str(values)
                try:
                   res = client.insert_rows_json(table_id, values)
                   result.append('OK ' + rate_to_get + '-' + from_date)
                except Exception as e:
                    return 'Error: {}'.format(str(e))
#            else:
#                result.append('No data for ' + rate_to_get + '-' + from_date)
        else:
            result.append('No data for ' + rate_to_get + '-' + from_date)
        time.sleep(15)
    return str(result)
