import os
from craigslist import CraigslistHousing
from slack import WebClient
from slack.errors import SlackApiError
#from slackclient import SlackClient
import time
import sys
import traceback

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean
from sqlalchemy.orm import sessionmaker
from dateutil.parser import parse



#client.chat_postMessage(channel=user, text=msg)


engine = create_engine('sqlite:///listings.db', echo=False)

Base = declarative_base()

class Listing(Base):
    """
    A table to store data on craigslist listings.
    """

    __tablename__ = 'listings'

    id = Column(Integer, primary_key=True)
    link = Column(String, unique=True)
    created = Column(DateTime)
    geotag = Column(String)
    name = Column(String)
    price = Column(String)
    #location = Column(String)
    cl_id = Column(Integer, unique=True)
    area = Column(String)

Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()


INTERVAL= 1*60

BOXES = {
    "adams_point": [
        [37.80789, -122.25000],
        [37.81589,	-122.26081],
    ],
    "piedmont": [
        [37.82240, -122.24768],
        [37.83237, -122.25386],
    ],
    "rockridge": [
        [37.83826, -122.24073],
        [37.84680, -122.25944],
    ],
    "berkeley": [
        [37.86226, -122.25043],
        [37.86781, -122.26502],
    ],
    "north_berkeley": [
        [37.86425, -122.26330],
        [37.87655, -122.28974],
    ],
    "pac_heights": [
        [37.79124, -122.42381],
        [37.79850, -122.44784],
    ],
    "lower_pac_heights": [
        [37.78554, -122.42878],
        [37.78873, -122.44544],
    ],
    "haight": [
        [37.77059, -122.42688],
        [37.77086, -122.45401],
    ],
    "sunset": [
        [37.75451, -122.46422],
        [37.76258, -122.50825],
    ],
    "richmond": [
        [37.77188, -122.47263],
        [37.78029, -122.51005],
    ],
    "presidio": [
        [37.77805, -122.43959],
        [37.78829, -122.47151],
    ]
}


def in_box(result_coords, box_coords):
    if box_coords[0][0] < result_coords[0] < box_coords[1][0] and box_coords[1][1] < result_coords[1] < box_coords[0][1]:
        return True
    return False

def interesting_area(results):
    geotag = results['geotag']
    area_found = False
    area=''
    for a, coords in BOXES.items():
        if in_box(geotag, coords):
            area = a
            area_found=True
    return {
        "area_found": area_found,
        "area": area
    }


def scrape_area():
    cl_h = CraigslistHousing(site='sfbay', area = 'sfc', category = 'apa', filters={'max_price':2000})
    results = []
    
    house_list = cl_h.get_results(sort_by='newest', geotagged='True', limit=3)

    while True:
        try: 
            result = next(house_list)
        except StopIteration:
            break
        except Exception:
            continue
        #if (interesting_area(result)["area_found"] == True):
        area = interesting_area(result)["area"]
        result['area']=area
        listing = session.query(Listing).filter_by(cl_id=result["id"]).first()
        if listing is None:
            if result["where"] is None:
                continue
            
            listing = Listing(
                link=result["url"],
                created=parse(result["datetime"]),
                name=result["name"],
                price=result["price"],
                # location=result["where"],
                cl_id=result["id"],
                area=interesting_area(result)["area"],
            )
            
            session.add(listing)
            session.commit()
        results.append(result)
                
    return results
                    
        
    
#Scraping HERE:
def do_scrape():

            #POSTING to SLACK
            SLACK_TOKEN = "xoxb-1444974159414-1452033369987-Nkx4FTGsGKeh9xZFCQJSrNm9"
            SLACK_CHANNEL = "#housing"
            sc = WebClient(token=SLACK_TOKEN)
            
            
            all_results= scrape_area()
            
            print(all_results)
            
            for result in all_results:
                desc = "{0} | {1} | {2} | <{3}>".format(result["area"], result["price"], result["name"], result["url"])
                try:
                    sc.chat_postMessage(
                    channel=SLACK_CHANNEL, text=desc)
                except SlackApiError as e:
                    print(e)
       
    
if __name__ == "__main__":
    while True:
        print("{}: Starting scrape cycle".format(time.ctime()))
        try:
            do_scrape()
        except KeyboardInterrupt:
            print("Exiting....")
            sys.exit(1)
        except Exception as exc:
            print("Error with the scraping:", sys.exc_info()[0])
            traceback.print_exc()
        else:
            print("{}: Successfully finished scraping".format(time.ctime()))
        time.sleep(INTERVAL)





