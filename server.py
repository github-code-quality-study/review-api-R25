import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
from urllib.parse import parse_qs, urlparse
import json
import pandas as pd
from datetime import datetime
import uuid
import os
from typing import Callable, Any
from wsgiref.simple_server import make_server
from datetime import datetime
nltk.download('vader_lexicon', quiet=True)
nltk.download('punkt', quiet=True)
nltk.download('averaged_perceptron_tagger', quiet=True)
nltk.download('stopwords', quiet=True)

adj_noun_pairs_count = {}
sia = SentimentIntensityAnalyzer()
stop_words = set(stopwords.words('english'))

reviews =  pd.read_csv('data/reviews.csv')

class ReviewAnalyzerServer:
    def __init__(self) -> None:
        # This method is a placeholder for future initialization logic
        self.reviews = reviews
        # Calculate the sentiment for newly added reviews
        self.reviews['sentiment'] = self.reviews['ReviewBody'].apply(sia.polarity_scores)

    def analyze_sentiment(self, review_body):
        sentiment_scores = sia.polarity_scores(review_body)
        return sentiment_scores

    def filter_reviews(self,location=None,start_date=None,end_date=None):

        if start_date:
            start_date = datetime.strptime(start_date,"%Y-%m-%d")
        if end_date:
            end_date = datetime.strptime(end_date,"%Y-%m-%d")


        result = self.reviews

        if location:
            result = result[result['Location'].str.contains(location,case=False)]
        if start_date and end_date:
            result['Timestamp'] = pd.to_datetime(result['Timestamp'])
            result = result[(result['Timestamp']>= start_date) & (result['Timestamp'] <= end_date)]
            result['Timestamp'] = result['Timestamp'].dt.strftime("%Y-%m-%d %H:%M:%S")
        elif start_date:
            result['Timestamp'] = pd.to_datetime(result['Timestamp'])
            result = result[(result['Timestamp']>= start_date)]
            result['Timestamp'] = result['Timestamp'].dt.strftime("%Y-%m-%d %H:%M:%S")
        elif end_date:
            result['Timestamp'] = pd.to_datetime(result['Timestamp'])
            result = result[(result['Timestamp'] <= end_date)]
            result['Timestamp'] = result['Timestamp'].dt.strftime("%Y-%m-%d %H:%M:%S")

        return result.to_dict('records')

    def __call__(self, environ: dict[str, Any], start_response: Callable[..., Any]) -> bytes:
        """
        The environ parameter is a dictionary containing some useful
        HTTP request information such as: REQUEST_METHOD, CONTENT_LENGTH, QUERY_STRING,
        PATH_INFO, CONTENT_TYPE, etc.
        """

        if environ["REQUEST_METHOD"] == "GET":

            query = parse_qs(environ.get("QUERY_STRING",""))
            location = query.get('location',[None])[0]
            start_date = query.get('start_date',[None])[0]
            end_date = query.get('end_date',[None])[0]

            fitered_reviews = self.filter_reviews(location,start_date,end_date)

            
            # Create the response body from the reviews and convert to a JSON byte string
            response_body = json.dumps(sorted(fitered_reviews,key=lambda x:x['sentiment']['compound'],reverse=True), indent=2).encode("utf-8")
            
            # Write your code here
            

            # Set the appropriate response headers
            start_response("200 OK", [
            ("Content-Type", "application/json"),
            ("Content-Length", str(len(response_body)))
             ])
            
            return [response_body]

        if environ["REQUEST_METHOD"] == "POST":
         
            try:
                request_body_size = int(environ.get('CONTENT_LENGTH',0))
            except:
                request_body_size = 0

            request_body = environ['wsgi.input'].read(request_body_size)
             
            data = parse_qs(request_body.decode('utf-8'))
             
            # Get the review body
            print(data)
            location = data.get('Location',[None])[0]
             
            review_body = data.get('ReviewBody',[None])[0]
            
            valid_locations = ['Albuquerque, New Mexico',
"Carlsbad, California",
"Chula Vista, California","Colorado Springs, Colorado","Denver, Colorado","El Cajon, California"
,"El Paso, Texas"
,"Escondido, California"
,"Fresno, California"
,"La Mesa, California"
,"Las Vegas, Nevada"
,"Los Angeles, California"
,"Oceanside, California"
,"Phoenix, Arizona"
,"Sacramento, California"
,"Salt Lake City, Utah"
,"Salt Lake City, Utah"
,"San Diego, California"
,"Tucson, Arizona"]
            
            if location and review_body  and location in valid_locations:
                review_id = str(uuid.uuid4())
                timestamp = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

                new_review = {
                    "ReviewId" : review_id,
                    "ReviewBody" : review_body,
                    "Location" : location,
                    "Timestamp" : timestamp
                }

 
                response_body = json.dumps( new_review, indent=2).encode("utf-8")
            
            
            

                # Set the appropriate response headers
                start_response("201 Created", [
                ("Content-Type", "application/json"),
                ("Content-Length", str(len(response_body)))
                ])
                
                return [response_body]
            else:
                start_response('400 Bad Request',[])
                return [b'Bad Reqeust: Missing ReviewBody or Location']
        

if __name__ == "__main__":
    app = ReviewAnalyzerServer()
    port = os.environ.get('PORT', 8000)
    with make_server("", port, app) as httpd:
        print(f"Listening on port {port}...")
        httpd.serve_forever()