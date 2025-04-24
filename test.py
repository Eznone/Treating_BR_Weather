from geopy.geocoders import Nominatim
    
geolocator = Nominatim(user_agent="geocoding_app")
coordinates = "52.509669, 13.376294"
location = geolocator.reverse(coordinates)

print(location.address)