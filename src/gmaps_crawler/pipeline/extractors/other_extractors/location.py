from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import time

city_details_cache = {}


def get_city_details(city_name, retries=3, delay=1):
    """
    Retrieves city, state/province, and country information based on a city name.
    Returns a tuple: (city, state, country)
    Default values are empty strings "".
    If no province/state, use city as both city and state.
    
    Retries automatically when GeocoderTimedOut occurs.
    """
    if city_name in city_details_cache:
        return city_details_cache[city_name]
    
    geolocator = Nominatim(user_agent="my-geocoder-app")

    for attempt in range(retries):
        try:
            location = geolocator.geocode(city_name, addressdetails=True, language='en')
            
            if location:
                address = location.raw.get('address', {})
                
                # Extract city
                city = address.get('city') or \
                       address.get('town') or \
                       address.get('village') or \
                       address.get('county') or \
                       address.get('province')
                
                # Extract state/province
                state = address.get('state') or address.get('province')
                
                # 如果没有state/province，就让state = city
                if not state and city:
                    state = city
                
                # Extract country
                country = address.get('country')
                
                # 默认返回空字符串而不是 None
                city = city or ""
                state = state or ""
                country = country or ""
                data = (city, state, country)
                city_details_cache[city_name] = data
                return data
            
            else:
                raise RuntimeError("Location not found.")
        
        except GeocoderTimedOut:
            # 超时重试
            if attempt < retries - 1:
                time.sleep(delay)
                continue
            else:
                raise RuntimeError("Geocoding service timed out after multiple attempts.")
        
        except GeocoderServiceError as e:
            raise RuntimeError(f"Geocoding service error: {e}")

        except RuntimeError as e:
            raise RuntimeError(f"Error: {e}, input city name: {city_name}")
        
        except Exception as e:
            # 其他未知错误
            raise RuntimeError(f"An unknown error occurred: {e}")
        
    
        

# Example Usage
if __name__ == "__main__":
    print("Input: Tokyo")
    print(f"Output: {get_city_details('Tokyo')}\n")

    print("Input: paris")
    print(f"Output: {get_city_details('Paris')}\n")

    print("Input: 337 Bleecker St, New York, NY 10014, United States")
    print(f"Output: {get_city_details('337 Bleecker St, New York, NY 10014, United States')}\n")