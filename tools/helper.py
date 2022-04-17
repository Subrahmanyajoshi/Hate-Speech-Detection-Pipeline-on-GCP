import requests


class Helper(object):

    """ This class contains functions to scrape review data from Amazon website"""

    HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/90.0.4430.212 Safari/537.36'}

    # Cookies need to be added from the webpage. They will be in the 'Network' tab of the 'Inspect' page in the webpage.
    # The format is COOKIES = {'key1': 'value1', 'key2': 'value2' ...}
    COOKIES = {}

    def __init__(self, user_query: str):
        self.user_query = user_query
        self.url = f"https://www.amazon.in/s?k={user_query}"

    @staticmethod
    def get_data(url: str):
        """ fetches the html data from the given URL and returns it. """

        page = requests.get(url, cookies=Helper.COOKIES, headers=Helper.HEADERS)
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


