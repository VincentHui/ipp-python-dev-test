from datetime import datetime
import re
from jsonschema import validate
import jsonschema
import numpy as np

from starlette.applications import Starlette
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.requests import Request
from starlette.routing import Route
import uvicorn

PAYLOAD_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "Date": {
                "type": "string",
                "pattern": r"^(0[1-9]|[12][0-9]|3[01])/(0[1-9]|1[0-2])/\d{4}$",
            },
            "OPEN": {"type": "number"},
            "CLOSE": {"type": "number"},
            "HIGH": {"type": "number"},
            "LOW": {"type": "number"},
        },
        "required": ["Date", "OPEN", "CLOSE", "HIGH", "LOW"],
        "additionalProperties": False,
    }
}



async def price_data(request: Request) -> JSONResponse:
    """
    Return price data for the requested symbol
    """

    arr = np.loadtxt("./data/nifty50_all.csv",
                        skiprows=1,
                        converters={ 0: lambda date: datetime.strptime(date.decode('UTF-8'), "%Y-%m-%d")},
                        delimiter=",", dtype=[('Date', datetime), ('Symbol', 'U25'), ('Close', '<f8'), ('Open', '<f8'), ('High', '<f8'), ('Low', '<f8')])

    # TODO:
    # 1) Return open, high, low & close prices for the requested symbol as json records
    # 2) Allow calling app to filter the data by year using an optional query parameter

    # Symbol data is stored in the file data/nifty50_all.csv
    if request.method == 'POST' :
        payloadArray = await request.json()
        symbol = request.path_params["symbol"]

        #validate payload
        try:
            validate(instance=payloadArray, schema=PAYLOAD_SCHEMA)
        except jsonschema.exceptions.ValidationError as err:
            return JSONResponse({"message": "incorrect imput json", "error": err.message}, status_code=400)
        #check if record exists
        def convert_date(Date):
            return datetime.strptime(Date, "%d/%m/%Y")
        if any(set((convert_date(itm["Date"]), symbol) for itm in payloadArray).intersection((itm["Date"], symbol) for itm in arr )) :
            return PlainTextResponse("Record already exists", 409)

        with open("data/nifty50_all.csv", "a") as myfile:
            myfile.writelines(f'{ convert_date(itm["Date"]).strftime("%Y-%m-%d") },{symbol},{itm["CLOSE"]},{itm["OPEN"]},{itm["HIGH"]},{itm["LOW"]}\n' for itm in payloadArray)
        return JSONResponse({'message': 'updated data', 'payload': payloadArray}, status_code=200)
    
    if request.method == 'GET' :
        #validate query params
        if "year" in request.query_params and re.match(r'.*([1-3][0-9]{3})', request.query_params['year']) is None :
            return PlainTextResponse('entered incorrect year', status_code=400)
        result = get_price_data(request, arr)

        if not any(result):
            return PlainTextResponse('no results, entered incorrect symbol', status_code=400)
        return JSONResponse(result)

def get_price_data(request, arr):
    
    symbol = request.path_params['symbol']
    filteredList = [itm for itm in arr if symbol is itm['Symbol']]
    if "year" in request.query_params :
        filteredList = [itm for itm in arr if request.query_params['year'] == itm['Date'].strftime("%Y")]

    sortedList = [ {'date':  itm['Date'].strftime("%d/%m/%Y"), 'open': itm['Open'],'high': itm['High'],'low': itm['Low'], 'closed': itm['Close']}
                   for itm in sorted(filteredList, key=lambda itm: itm["Date"].timestamp(), reverse=True)]
                   
    return sortedList

# URL routes
app = Starlette(debug=True, routes=[
    Route('/nifty/stocks/{symbol}', price_data, methods=["GET", "POST"])
])

def main() -> None:
    """
    start the server
    """
    uvicorn.run(app, host='0.0.0.0', port=8888)


# Entry point
main()
