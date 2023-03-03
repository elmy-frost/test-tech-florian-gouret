import pandas as pd
import json
from datetime import date
import datetime
from io import StringIO
import time


def format_date_of_interest(from_date:str, to_date:str) -> str:
    """Ensure the dates are correct and in the good order

    Args:
        from_date (str): from
        to_date (str): to

    Returns:
        str: end of the url for the querry
    """
    
    if type(from_date) == datetime.date and type(to_date) == datetime.date:
        if from_date > to_date:
            from_date, to_date = to_date, from_date
        return f"?from={from_date.strftime('%d-%m-%Y')}&to={to_date.strftime('%d-%m-%Y')}"
    return ""


def extractor(data:str, format:str) -> json:
    """Extract a dataframe from a raw inpu

    Args:
        data (str): raw data
        format (str): format of the fetched data (json, csv)

    Returns:
        json: dataframe newly fetched
    """
    if format == "json":
        return pd.DataFrame(json.loads(data))
    
    if format == "csv":
        return pd.read_csv(StringIO(data))
    
    
def export(data: pd.DataFrame, format:str) :
    """Export the data into a given format

    Args:
        data (pd.DataFrame): _description_
        format (str): _description_

    Returns:
        _type_: _description_
    """
    if format == "json":
        return data.to_json(orient="records")
    
    if format == "csv":
        return data.to_csv(index=False)
    
    
def get_power(centrals: list) -> pd.DataFrame:
    """Aggregate the power of the centrals per timestamp

    Args:
        centrals (list): list of all centrals to consider

    Returns:
        pd.DataFrame: dataframe with power per timestamp
    """
    df = pd.concat(centrals)
    df = df.groupby("start")["power"].sum().reset_index()
    df = df.assign(
        end=df.start + 900,
        power=df.power.astype(int)
    )
    return df[["start", "end", "power"]]


def get_power_from_to(centrals: list, from_date: str, to_date: str, format: str):
    """Print the power per timestamp over a period of time

    Args:
        centrals (list): _description_
        from_date (_type_): _description_
        to_date (_type_): _description_
        format (_type_): _description_
    """
    
    centrals_data = []
    
    for central in centrals:
        central.set_date(from_date, to_date)
        if central.fetch():
            central.extract()
            central.transform()
            centrals_data.append(central.data)
        else:
            print("Service not available now for the central", central.name)
    
    if len(centrals_data) > 0:
        data_power = get_power(centrals_data)
        print(export(data_power, format))
    else:
        print("No service available now")
        