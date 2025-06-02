import functions_framework
import requests
import sqlalchemy
from sqlalchemy import insert
from sqlalchemy import text
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time


@functions_framework.http
def test(request):
##############Connection DB###################################
    connection_name = "riksbanken:europe-north2:riksbanken-001"
    db_name = "fx"
    db_user = "fx"
    db_password = "MNOmno001"

    driver_name = 'mysql+pymysql'
    query_string = dict({"unix_socket": "/cloudsql/{}".format(connection_name)})

    db = sqlalchemy.create_engine(
      sqlalchemy.engine.url.URL(
        drivername=driver_name,
        database=db_name,
        username=db_user,
        password=db_password,
        query=query_string,
      ),
      pool_size=5,
      max_overflow=2,
      pool_timeout=30,
      pool_recycle=1800
    )

#######################################################################################
## Figure out latest values
#######################################################################################

    def get_latest_date(rate_to_get):
        with db.connect() as conn:
            sql = "SELECT max(from_date) as latest FROM exchangerates WHERE currency = %s"
            latest_value = conn.execute(sql, (rate_to_get))
 
            latest_value_string = str(latest_value.fetchall())
            date_part_string = latest_value_string[16:-4]
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
    exchangerates_to_get = ['AUD','CAD','EUR','GBP','MXN','NZD','SGD','USD','DKK','JPY','NOK']
    
    result = []
    for rate_to_get in exchangerates_to_get:
        
        seriesId = 'SEK' + rate_to_get + 'PMI'
        from_date = get_latest_date(str(rate_to_get))
#        return 'https://api.riksbank.se/swea/v1/ObservationAggregates/' + seriesId + '/M/' + from_date   
        response = requests.get('https://api.riksbank.se/swea/v1/ObservationAggregates/' + seriesId + '/M/' + from_date)
    
        if str(response) != "<Response [204]>":        
            response_json = response.json()
            if len(response_json) > 0:
                values = [{ 'currency': rate_to_get,
                    'from_date': response_json[value]["from"], 
                    'to_date': response_json[value]["to"], 
                    'average_value': response_json[value]["average"], 
                    'min_value': response_json[value]["min"], 
                    'max_value': response_json[value]["max"], 
                    'ultimo_value': response_json[value]["ultimo"]} 
                for value in range(0, len(response_json), 1)]
    
                try:
                    with db.connect() as conn:
                        conn.execute(
                            text("INSERT INTO exchangerates (currency, from_date, to_date, average_value, min_value, max_value, ultimo_value) VALUES (:currency, :from_date, :to_date, :average_value, :min_value, :max_value, :ultimo_value)"),
                            values,
                        )
                    result.append('OK ' + rate_to_get + '-' + from_date)
                except Exception as e:
                    return 'Error: {}'.format(str(e))
            else:
                result.append('Err ' + rate_to_get + '-' + from_date)
        else:
            result.append('Err ' + rate_to_get + '-' + from_date)
        time.sleep(15)
    #if len(result) > 0:
    return str(result)
    #else:
    #    return 'No data to get for any of our currencies'
