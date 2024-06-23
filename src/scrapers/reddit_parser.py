import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from urllib.parse import urljoin, urlencode
import json

PERIOD_TO_GET = 60 * 60 * 24 
SUBREDDIT_PATH = '/r/gradadmissions/top/?sort=top&t=day'


class RedditScraper:
    BASE_URL = "https://old.reddit.com"

    def __init__(self, max_concurrent_tabs = 15 ,user_agent = None):
        self.results = []
        self.max_concurrent_tabs = max_concurrent_tabs
        self.semaphore = asyncio.BoundedSemaphore(self.max_concurrent_tabs)
        self.user_agent  = user_agent
    
    async def check_date_in_period(self, post_create_date, last_parsed_time, period):
        if period:
            # Convert milliseconds to seconds and create timezone-aware datetime objects
            post_create_date_datetime = datetime.fromtimestamp(post_create_date / 1000, tz=timezone.utc)
            last_parsed_time_datetime = last_parsed_time.astimezone(timezone.utc)
            
            # Calculate the time difference in seconds
            time_difference = (last_parsed_time_datetime - post_create_date_datetime).total_seconds()

            return time_difference <= period

        return True

    def save_as_json(self, obj, filename, mode='w', indent=4):
        with open(filename, mode) as file:
            json.dump(obj, file, indent=indent)

    async def get_posts(self, page, subreddit_path, parse_all  = False , count_limit = None , period_to_get = None, attributes_to_extract=['data-fullname', 'data-timestamp', 'data-permalink', 'data-promoted']):
        continue_parsing = True
        subpages = ''        
        self.results = []
        check_for_count = True

        if not period_to_get:
            print(f"Period not detected, parsing whole 1000 post from - {subreddit_path}")

        if not count_limit:
            check_for_count = False
            count_limit = float('inf')


        while continue_parsing:

            url_to_check  = RedditScraper.BASE_URL + subreddit_path + subpages
            print(f"Parsing {url_to_check}")
            await page.goto(url_to_check)
            # await page.screenshot(path =  "test2.png")   
            # await page.wait_for_selector('.thing')

            page_contains_thing_class = await page.query_selector('.thing')

            if not page_contains_thing_class:
                print(f"Blank url -{url_to_check}")
                print(f"Probably 1000 post limit exceeded. Aborting ...")
                return self.results

            selected_elements = await page.evaluate(
                '''
                (attributes_to_extract) => Array.from(document.querySelectorAll('div[data-fullname]')).map(element => {
                    const obj = {};
                    attributes_to_extract.forEach(attr => {
                        obj[attr] = element.getAttribute(attr) || null;
                    });
                    return obj;
                })
                ''',
                attributes_to_extract,
            )

            if len(selected_elements) < 25:
                continue_parsing = False

            for row in selected_elements:

                if row.get('data-promoted') == 'true':
                    continue
                
                if parse_all:
                    pass

                elif check_for_count:
                    
                    if len(self.results) >= count_limit:
                        continue_parsing = False
                        break
                    
                else:
                    is_included_in_period = await self.check_date_in_period(
                        int(row['data-timestamp']), datetime.now(), period_to_get
                    )

                    if not is_included_in_period:
                        continue_parsing = False
                        break
                
                row.pop('data-promoted')
                self.results.append(row)
            
            params = {"count": 25, "after": selected_elements[-1].get('data-fullname')}
            subpages = urljoin('/', '?' + urlencode(params)) if not 'top' in subreddit_path else urljoin('&' ,'&' + urlencode(params)) 

        print(f'Amount of posts to be parsed - {len(self.results)}')

        return self.results

    async def parse_subreddit(self, subreddit_path , parse_all = False, count_limit = None, period = None, close_visual_browser = True):
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=close_visual_browser)

            context = await browser.new_context( no_viewport=True
                                                ,user_agent=self.user_agent
                                                )
            page = await context.new_page()

            await self.get_posts(page = page, 
                                 parse_all= parse_all,
                                 count_limit=count_limit,
                                 subreddit_path = subreddit_path, 
                                 period_to_get = period or PERIOD_TO_GET)

            tasks = [self.process_post(result, context) for result in self.results]


            for future in asyncio.as_completed(tasks):
                result = await future
                yield result

    async def process_post(self, result, context):
        
        comment_link = RedditScraper.BASE_URL + result.get('data-permalink')
        print(f"Parsing comments for - {comment_link}")
        comment_data = await self.parse_post_data(comment_link, context, close_tab=True)

        return comment_data
                
    async def parse_post_data(self, comment_path, context, close_tab=True):
        async with self.semaphore:
            page = await context.new_page()
            await page.goto(comment_path)

            await page.wait_for_load_state('load')
            
            html_content = await page.content()
            soup = BeautifulSoup(html_content, 'html.parser')

            heading_data = await self.get_heading_data(soup)
            
            comments = [
                comment async for comment in self.get_comments_data(soup=soup, parent_post_id=heading_data.get('Id'))
            ]


            heading_data['RelatedCommentId'] = {'id' : [comment_id.get('Id') for comment_id in comments ]}

            if close_tab:
                await page.close()

            return [heading_data] + comments


    async def get_comments_data(self, soup, parent_post_id=None):
        comment_divs = soup.find_all('div', class_='thing', attrs={'data-type': 'comment'})

        for comment_div in comment_divs:
            parent_data_fullname = None
            data_fullname = comment_div.get('data-fullname')
            data_author = comment_div.get('data-author')
            data_author_id = comment_div.get('data-author-fullname')
            data_permalink = comment_div.get('data-permalink')

            comment_text = comment_div.find('div', class_='md').get_text(
                strip=True) if comment_div.find('div', class_='md') else None

            timestamp_element = comment_div.find('time', class_='live-timestamp')
            timestamp = int(
                datetime.fromisoformat(timestamp_element['datetime']).timestamp() * 1000) if timestamp_element else None

            parent_div = comment_div.find_parent(
                'div', class_='thing', attrs={'data-type': 'comment'})

            if parent_div:
                parent_data_fullname = parent_div['data-fullname']

            yield {
                "Type": "Comment",
                "Id": data_fullname,
                "AuthorId": data_author_id,
                "Author": data_author,
                'Permalink': data_permalink,
                'Comment': comment_text,
                'ParentCommentId': parent_data_fullname,
                "ParentPostId": parent_post_id,
                "CreatedAt": timestamp
            }

    async def get_heading_data(self, soup):
        data_fullname = soup.select_one('.thing').get('data-fullname')
        permalink = soup.select_one('.title a').get('href')
        timestamp = soup.select_one('.thing').get('data-timestamp')
        data_author = soup.select_one('.thing').get('data-author')
        data_author_id = soup.select_one('.thing').get('data-author-fullname')

        post_text = soup.select_one('.thing .md').get_text(
            strip=True) if soup.select_one('.thing .md') else None

        return {
            "Type": "Post",
            "Id": data_fullname,
            "AuthorId": data_author_id,
            "AuthorName": data_author,
            'Permalink': permalink,
            'Comment': post_text,
            "CreatedAt": timestamp
        }

if __name__ == '__main__':
    async def main():
        scraper = RedditScraper()
        async for result in scraper.parse_subreddit(SUBREDDIT_PATH, parse_all = True, period = 60 * 60 * 24 * 10):
            # Process each result as it becomes available
            print(result)

            test =1
        
    asyncio.run(main())