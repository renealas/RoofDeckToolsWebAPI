from google.cloud import firestore
from google.cloud import storage
import requests
from bs4 import BeautifulSoup
from typing import List 
from googlemaps import Client as GoogleMaps
from google.cloud.firestore import GeoPoint
from dataclasses import dataclass
from dataclasses_json import dataclass_json, config
import pygeohash as gh
import hashlib
from datetime import datetime
import urllib.request
from PIL import Image
import os.path
import shutil
import random
import string
import sys
import functions_framework
from flask import jsonify

#Images Folder Path:
folder = '/tmp/'

@dataclass_json
@dataclass
class Listing:
    status: str
    propertyType: str
    street: str
    street2: str
    city: str
    county: str
    state: str
    zipcode: int
    yearBuilt: int
    interior: float
    exterior: float
    bedrooms: int
    bathrooms: float
    listingType: str
    listingAgentName: str
    listingAgentEmail: str
    listingAgentPhone: int
    photos: list
    latitude: float
    longitude: float
    position: dict()
    hash: str 
    description: str
    price: int
    bedPlus: bool
    bathPlus: bool
    zone: List[str]

def dl_jpg(url, file_path, file_name):
    full_path = file_path + file_name + '.jpg'
    path = urllib.request.urlretrieve(url, full_path)

def find_between( s, first, last ):
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
        return s[start:end]
    except ValueError:
        return ""

def optimizeImage(name) -> str:
    foo = Image.open(os.path.join('/tmp/', name + '.jpg'))
    foo = foo.resize((525,394),Image.ANTIALIAS)
    foo.save('/tmp/' + name + '.jpg',optimize=True,quality=50)
    print('Optimized Image: ' + name)
    return '/tmp/' + name + '.jpg'

def random_name() -> str:
    # printing lowercase
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(10)) 

#This is the Entry point in Cloud Functions Google 
@functions_framework.http
def scrubbing(request):

    # For more information about CORS and CORS preflight requests, see:
    # https://developer.mozilla.org/en-US/docs/Glossary/Preflight_request

    # Set CORS headers for the preflight request
    if request.method == 'OPTIONS':
        # Allows GET requests from any origin with the Content-Type
        # header and caches preflight response for an 3600s
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }

        print('Error in Headers')
        print(headers)

        return ('Error in Headers', 204, headers)

    # Set CORS headers for the main request
    headers = {
        'Content-Type':'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
    }

    # END CORS

    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'city' in request_json:
        city = request_json['city']
    elif request_args and 'city' in request_args:
        city = request_args['city']
    if request_json and 'webhash' in request_json:
        webhash = request_json['webhash']
    elif request_args and 'webhash' in request_args:
        webhash = request_args['webhash']
    else:
        data = {'Status': 'Error', 'Error': 'No City Given, or Web Hash Given'}
        return jsonify(data), 400

    #city = request.args.get('city')
    #city = request.body.city;

    website = requests.get(f'https://www.compass.com/homes-for-sale/'+city+'-ny/status=coming-soon/')
    soup = BeautifulSoup(website.text, 'html.parser')

    listings = []

    #Here we do the Scrubbing

    #Status, Property Type, Year Built and County
    propertyInfo = []
    urls = []

    cards = soup.find_all('div', {'class': 'uc-listingPhotoCard'})

    for card in cards:
        info = soup.find_all('script', {'type': 'application/ld+json'})

        for info in info:
            information =  info.text.split(',')
            str_match = [s for s in information if "url" in s]
            if len(str_match) > 1:
                urlInfo = str_match[0]
                url = urlInfo[7:-1]
                if url in urls:
                    pass
                else:
                    urls.append(url)
    
    for url in urls:
        website = requests.get(url)
        soup = BeautifulSoup(website.text, 'html.parser')

        #Status, Property Type, Year Built and County
        propertyInfo = []
        tableDetails = soup.find_all('tr', {'class': 'keyDetails-text'})
        for detail in tableDetails:
            propertyInfo.append(detail.find('td').text)
        
        status = propertyInfo[0]

        # this field is called typeInfo because we will do a switch case to find if coop the property type
        # will be call Condo/Coop and check for other properties
        try:
            typeInfo = propertyInfo[6]
            propertyTypes = {
            'House': 'Single',
            'Multi-Family': 'Single',
            'Multi Family': 'Single',
            'Single Family': 'Single',
            'Condo': 'Condo',
            'Condop': 'Condo',
            'Co-op': 'Condo',
            'Other': 'Condo',
            'Townhouse': 'Townhouse',
            'Land': 'Land',
            }
            propertyType = propertyTypes[typeInfo]
        except:
            propertyType = 'Condo'
        try:
            yearBuilt = int(propertyInfo[8])
        except:
            yearBuilt = 2020
        
        try:
            county = propertyInfo[9]
        except:
            county = 'New York County'

        #Address Info (except for County)
        addressContainer = soup.find('div', {'class': 'summary__Content-e4c4ok-3'})
        addressStreetInfo = addressContainer.find('p', {'class': 'summary__StyledAddress-e4c4ok-8'}).text
        streetsInfo = addressStreetInfo.split(',')

        if len(streetsInfo) > 1:
            street1 = streetsInfo[0].strip()
            street2 = streetsInfo[1].strip()
        else:
            street1 = streetsInfo[0].strip()
            street2 = ''  

        addressInfo = addressContainer.find('span', {'class': 'summary__StyledAddressSubtitle-e4c4ok-9'}).text  

        splittedAddress = addressInfo.split(',')

        if len(splittedAddress) > 2:

            city = splittedAddress[1].strip()

            stateZipInfo = splittedAddress[2].strip()
            splitStateZipInfo = stateZipInfo.split(' ')
            state = splitStateZipInfo[0].strip()
            zipcode = int(splitStateZipInfo[1].strip())

            if len(streetsInfo) > 1:
                addressFull = street1 + ', ' + street2 + ', ' + city + ', ' + state + ', ' + str(zipcode)
            else:
                addressFull = street1 + ', ' + city + ', ' + state + ', ' + str(zipcode)
        else:

            city = splittedAddress[0].strip()

            stateZipInfo = splittedAddress[1].strip()
            splitStateZipInfo = stateZipInfo.split(' ')
            state = splitStateZipInfo[0].strip()
            zipcode = int(splitStateZipInfo[1].strip())

            if len(streetsInfo) > 1:
                addressFull = street1 + ', ' + street2 + ', ' + city + ', ' + state + ', ' + str(zipcode)
            else:
                addressFull = street1 + ', ' + city + ', ' + state + ', ' + str(zipcode)
        
        #Property Details (Price, Bedrooms, Bathrooms, Interior SqFootage)
        propertyDetailsInfo = soup.find_all('div', {'class': 'textIntent-title2'})
        details = []
        for detail in propertyDetailsInfo:
            details.append(detail.text)

        price = float(details[0].lstrip('$').replace(',',''))

        if(propertyType == 'Land'):
            bedrooms = 0
            bathrooms = 0.00
            splitInterior = details[1].split('/')
            rawInterior = float(splitInterior[1].strip())
            interior = 0.00
            exterior = round((rawInterior*4) /4) #Presicion is not adequate 
        else:
            if(details[1].isdigit()):
                bedrooms = int(details[1])
            else:
                bedrooms = 1           
        
            if(details[2].isdigit()): 
                bathrooms = float(details[2])
            else: 
                bathrooms = 1.00
            interiorX = details[4].replace(',','') 
            try:
                interior = float(interiorX) 
            except:
                interior = 0.00
            exterior = 0.00

        #Listing Agent Info
        listingType = 'Web'

        contactAgentName = soup.find('p', {'class': 'textIntent-title2'}).text
        #contactInfo = soup.find('div', {'class': 'contact-agent__StyledContactTextContainer-aj3bbe-6 gOYaAS'})

        try:
            mailX = soup.find('a', {'class': 'contact-agent-slat__StyledEmailLink-l633vc-9'}).text
            phone = soup.find('p', {'class': 'contact-agent-slat__StyledContactInfo-l633vc-10'}).text
            contactEmail = mailX
            contactPhone1 = phone.lstrip('P: ').replace('.','')
            
            if '(' in contactPhone1:
                contactPhone = phone.lstrip('P: ').replace('.','').replace('(','').replace(')','').replace('-','')
            else:
                contactPhone = int(phone.lstrip('P: ').replace('.','').strip())
        except:
            contactEmail = 'sales@roofdeck.io'
            contactPhone = int('3052563236')
        
        #Images Section
        imagesRaw = []
        imagesSection = soup.find('div', {'class': 'src__GalleryContainer-sc-bdjcm0-7'})
        imagesInfo = imagesSection.find_all('img', {'class': 'gallery-image__StyledImg-sc-jtk816-0'})

        image1 = imagesInfo[0].get('src')

        for image in imagesInfo:
            img = image.get('data-flickity-lazyload-src')
            imagesRaw.append(img)

        imagesRaw.pop(0)
        imagesRaw.insert(0, image1)

        images = imagesRaw[:12]

        imageFile = []
                
        #Here we will store the images in local file
        for image in images:
            #First we change the ending from webp to jpg
            newURL = image[:-4] + 'jpg'
            print(newURL)
            
            name = find_between(newURL, "_img", "/origin.jpg")

            if name == "":
                name = random_name()

            print(name)
            #Here the function to download the image
            try:
                dl_jpg(newURL, '/tmp/', name)
            except:
                break
            #Here we Optimize the image to size 500 x 394 pixels
            # And get the location for the new image
            try:
                path = optimizeImage(name)
            except:
                break
            # We append the path to the Array of paths
            imageFile.append(path)

        #Get Description
        try:
            description = soup.find('span', {'class': 'sc-pIJJz APtCt'}).text
        except:
            description = 'Welcome to this new Listing in RoofDeck, this property is the perfect match for you and your family'

        #Get Price
        try:
            priceData = soup.find('div', {'class': 'textIntent-title2'}).text
        except:
            priceData = '$1,000,000'

        priceInfo = priceData.replace('$', '')

        price = int(priceInfo.replace(',', ''))

        #Here we check if there is above 6 Bathrooms or Bedrooms for the plus
        if(bedrooms > 6 ): 
            bedplus = True
        else:
            bedplus = False

        if(bathrooms > 6 ): 
            bathplus = True
        else:
            bathplus = False
        
        #Zone
        zone = [
            city,
            state
        ]

        #Here we get the Latitude and Longitude values
        gmaps = GoogleMaps(key='AIzaSyDj95hwES80jypayRWmBPbuSePIIm79z5U')
        geocode_result = gmaps.geocode(addressFull)

        # print('Results')
        # print(geocode_result)
        # print('----------------------------------------------------')

        latitude = geocode_result[0]['geometry']['location']['lat']
        longitude = geocode_result[0]['geometry']['location']['lng']

        #Get GeoPoint
        geopoint = GeoPoint(latitude, longitude)

        geocodeHash = gh.encode(latitude, longitude, precision = 9)

        position = {
            'geohash': geocodeHash,
            'geopoint': geopoint
        }
        
        #MD5 hash of the Address
        fullAdress = addressFull

        hash = hashlib.sha256(fullAdress.encode('utf8')).hexdigest()

        if len(images) > 0:
            listings.append(Listing(status, propertyType, street1, street2, city, county, state, zipcode, yearBuilt, interior, exterior, bedrooms, bathrooms, listingType, contactAgentName, contactEmail, contactPhone, imageFile, latitude, longitude, position, hash, description, price, bedplus, bathplus, zone))

    if len(listings) > 12:
        del listings[12:len(listings)]

    listingAmount = len(listings)

    #Here we do the insertion to the DB
    db = firestore.Client()

    store = storage.Client()

    today = datetime.today()

    homeFeatures = []

    listingsResponse = []

    for listing in listings:

        try:
            ref = db.collection('listings').document()

            photos = []

            print('Amount of photos: ' + str(len(listing.photos)))
            i = 0

            for image in listing.photos:
                fullpath = image #find_between(image, 'scrapping/', '.jpg') + '.jpg'

                fullpath2 = fullpath[1:]
                filename = fullpath2.split('/',1)[1]
                path = '/tmp'

                imagePath = path + '/' + filename
                bucket = store.get_bucket('testscrapping-2ab86.appspot.com')
                blob = bucket.blob('ListingImages/' + ref.id + '/' + filename)
                blob.upload_from_filename(imagePath)
                blob.make_public()
                photos.append(blob.public_url)
                i = i + 1
                print(i)
                print('------------')
                os.remove(imagePath)
                print('Removed File from Memory')

            db.collection('listings').document(ref.id).set({
            'photos': photos,
            'county': listing.county,
            'street': listing.street,
            'street2': listing.street2,
            'city': listing.city,
            'state': listing.state,
            'zipCode': listing.zipcode,
            'price': listing.price,
            'bedRooms': listing.bedrooms,
            'bedPlus': listing.bedPlus,
            'bathRooms': listing.bathrooms,
            'bathPlus': listing.bathPlus,

            'constrSize': listing.exterior,
            'lotSize': listing.interior,

            'description': listing.description,

            'listingType': 'Web',
            'listingAgentName': listing.listingAgentName,
            'listingAgentEmail': listing.listingAgentEmail,
            'listingAgentPhone': listing.listingAgentPhone,
            'homeFeatures': homeFeatures,
            'latitude': listing.latitude,
            'longitude': listing.longitude,
            'position': listing.position,
            'propertyType': listing.propertyType,
            'yearBuilt': listing.yearBuilt,
            'status': 'Listed',
            'zone': listing.zone,
            'lotAcreage': 0.0,
            'lotSqft': 0.0,
            'saves': 0,
            'views': 0,
            'hash': listing.hash,
            'webhash': webhash,
            #None Use Variables but needed on DB
            'schoolDistric': '', #School District not use in new Cole's set up.
            'taxes': 0,
            'floorPlans': '',
            'propertySurvey': '',
            'email': '',
            'name': '',
            'userId': '',
            'username': '',
            'userPhone': 0,
            'imageUrl': '',
            'dayStore': today,
            'downPayment': '',
            'url': '',
            })
            #This to add the Listing modified to the response
            listingsResponse.append({
            'id': ref.id,
            'photos': photos,
            'county': listing.county,
            'street': listing.street,
            'street2': listing.street2,
            'city': listing.city,
            'state': listing.state,
            'zipCode': listing.zipcode,
            'price': listing.price,
            'bedRooms': listing.bedrooms,
            'bedPlus': listing.bedPlus,
            'bathRooms': listing.bathrooms,
            'bathPlus': listing.bathPlus,

            'constrSize': listing.exterior,
            'lotSize': listing.interior,

            'description': listing.description,

            'listingType': 'Web',
            'listingAgentName': listing.listingAgentName,
            'listingAgentEmail': listing.listingAgentEmail,
            'listingAgentPhone': listing.listingAgentPhone,
            'homeFeatures': homeFeatures,
            'latitude': listing.latitude,
            'longitude': listing.longitude,
            #'position': listing.position,
            'propertyType': listing.propertyType,
            'yearBuilt': listing.yearBuilt,
            'status': 'Listed',
            'zone': listing.zone,
            'lotAcreage': 0.0,
            'lotSqft': 0.0,
            'saves': 0,
            'views': 0,
            'hash': listing.hash,
            'webhash': webhash,
            #None Use Variables but needed on DB
            'schoolDistric': '', #School District not use in new Cole's set up.
            'taxes': 0,
            'floorPlans': '',
            'propertySurvey': '',
            'email': '',
            'name': '',
            'userId': '',
            'username': '',
            'userPhone': 0,
            'imageUrl': '',
            'dayStore': today,
            'downPayment': '',
            'url': '',
            })
        except Exception as e:
            data = {'Status': 'Error', 'Error': str(e)}
            #response.status(500).send(result)
            return jsonify(data), 500 

    result = listingsResponse
    data = {'Status': 'Successful', 'Objects': result, 'Amount of Listings': str(len(result))} 
    #response.status(200).result(result)
    return jsonify(data), 201