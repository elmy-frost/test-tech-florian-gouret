from central import Central
import sys
from utils import get_power_from_to


properties_hawes = {
    "delimiters": ["start", "end", "power"],
    "source": "https://interview.beta.bcmenergy.fr/hawes",
    "format": "json"
    }

properties_hounslow = {
    "delimiters": ["debut", "fin", "valeur"],
    "source": "https://interview.beta.bcmenergy.fr/hounslow",
    "format": "csv"
    }

properties_barnsley = {
    "delimiters": ["start_time", "end_time", "value"],
    "source": "https://interview.beta.bcmenergy.fr/barnsley",
    "format": "json"
    }


barnsley = Central(properties_barnsley, name="barnsley")
hounslow = Central(properties_hounslow, name="hounslow")
hawes = Central(properties_hawes, name="hawes")

if __name__ == "__main__":
    start = sys.argv[1]
    end = sys.argv[2]
    format = sys.argv[3]
    get_power_from_to([barnsley, hounslow, hawes], start, end, format)
    