import requests
from bs4 import BeautifulSoup

from pubsub_publisher import PubSubPublisher


class ReviewScraper(object):

    """ This class contains functions to scrape review data from Amazon website"""

    HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/90.0.4430.212 Safari/537.36'}

    # Cookies need to be added from the webpage. They will be in the 'Network' tab of the 'Inspect' page in the webpage.
    # The format is COOKIES = {'key1': 'value1', 'key2': 'value2' ...}
    COOKIES = {}

    def __init__(self, user_query: str, project_id: str, topic_id: str):
        self.user_query = user_query
        self.base_url = f"https://www.amazon.in/s?k={user_query}"
        self.publisher = PubSubPublisher(project_id=project_id, topic_id=topic_id)

    @staticmethod
    def get_data(url: str):
        """ fetches the html data from the given URL and returns it. """

        page = requests.get(url, cookies=ReviewScraper.COOKIES, headers=ReviewScraper.HEADERS)
        return page

    @staticmethod
    def asin_number(soup):
        """ fetches asin_number (A unique identification number given to each product in amazon) of each of the
        product in the search results """

        data_asins = []
        for item in soup.find_all("div", {"data-component-type": "s-search-result"}):
            data_asins.append(item['data-asin'])
        return data_asins

    @staticmethod
    def fetch_href(soup):
        """ At the bottom of the webpage of every product, there will be a link 'all_reviews'. We need to go to this
        page to start scarping the reviews. This function fetches the same link for all of the products in the search
        results using the asin_numbers """

        links = []
        for item in soup.findAll("a", {'data-hook': "see-all-reviews-link-foot"}):
            links.append(item['href'])

        return links[0]

    @staticmethod
    def customer_review(soup):
        """ fetches all of the reviews from the all_reviews page. """

        data_str = ""
        for item in soup.find_all("span", class_="a-size-base review-text review-text-content"):
            data_str = data_str + item.get_text()

        result = data_str.split("\n")
        return result

    def run(self):

        response = ReviewScraper.get_data(self.base_url)
        soup = BeautifulSoup(response.content)

        # Fetch all of data asin ids
        data_asins = ReviewScraper.asin_number(soup)
        message_count = 0

        for data_asin in data_asins[0:2]:
            url = f"https://www.amazon.in/dp/{data_asin}"

            # Extract all_reviews link for each of the product
            response = ReviewScraper.get_data(url)
            soup = BeautifulSoup(response.content)
            link = ReviewScraper.fetch_href(soup)

            # Fetch all of the reviews from the extracted all_reviews link
            i = 0
            
            print(f"Fetching reviews for the product: {data_asin}")
            while 1:
                i += 1
                url = f"https://www.amazon.in{link}&pageNumber={i}"
                response = ReviewScraper.get_data(url)
                soup = BeautifulSoup(response.text)
                review_data = ReviewScraper.customer_review(soup)
                review_data = [review for review in review_data if len(review) > 0]
                if len(review_data) == 0:
                    break

                for review in review_data:
                    message = {'review': review}
                    self.publisher.publish(message=message)
                    message_count += 1

                print(f"Total reviews published till now: {message_count}")

                
if __name__ == '__main__':

    scraper = ReviewScraper(user_query='boat+earphones', project_id='text-analysis-323506', topic_id='reviews-texts')
    scraper.run()