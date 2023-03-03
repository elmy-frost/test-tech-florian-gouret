import pandas as pd
import requests
import json
import datetime
import time
from utils import *



class Central:
    """
    Central class where the different step for the preprocessing are stored
    """
    
    def __init__(self, properties: dict, name: str) -> None:
        """Initialyse the object 

        Args:
            properties (dict): properties of a  given central
            name (str): name of the central
        """
        if "delimiters" not in properties or "source" not in properties or "format" not in properties:
            raise "Creation de l'entree impossible"
        
        self.name = name
        self.delimiters: dict = properties["delimiters"]
        self.source: str = properties["source"]
        self.format: str = properties["format"]
        self.raw_data: list = []
        self.data: pd.DataFrame = pd.DataFrame()
        self.from_date: datetime
        self.to_date: datetime
    
    def set_date(self, ifrom_date, ito_date):
        """
        Setter
        """
        ifrom_date = ifrom_date.split("-")
        from_date = datetime.date(int(ifrom_date[2]), int(ifrom_date[1]), int(ifrom_date[0]))
        ito_date = ito_date.split("-")
        to_date = datetime.date(int(ito_date[2]), int(ito_date[1]), int(ito_date[0]))

        self.from_date = from_date
        self.to_date = to_date
    
    
    def fetch(self) -> bool:
        """Fetche raw data from the source

        Returns:
            bool: success of the fetching
        """
        
        date_time = format_date_of_interest(self.from_date, self.to_date)
        
        if date_time == "":
            raise "An error with the dates occurred."

        request = requests.get(self.source + date_time)    
        
        if request.status_code != 200: return False
            
        self.raw_data = request.text
        return True
    
    
    def extract(self) -> None:
        """
        Extracte the data from the raw contain based on the source format
        """
        if self.raw_data == []:
            return 
        
        self.data = extractor(self.raw_data, self.format)
        self.rename()
        
        
    def rename(self) -> None:
        """
        Casting the columns
        """
        if self.raw_data == []:
            return 
        self.data = self.data.rename(columns=
            {
                self.delimiters[0]: "start", 
                self.delimiters[1]: "end",
                self.delimiters[2]: "power"
            }
        )
    
    def transform(self) -> None:
        """
        Transform the data into the correct format 
        """
        if self.raw_data == []:
            return 
        time_start = int(time.mktime(self.from_date.timetuple()))
        time_end = int(time.mktime(self.to_date.timetuple()))
        reference = pd.DataFrame({"start":[i for i in range(time_start, time_end, 900)]})
        self.data = pd.merge(left=reference, right=self.data, how="left", on="start").ffill()
        
    def handle_missing_values(self):
        """
        to be implemented later
        no missing values available throughout the different runs
        """
        pass
    