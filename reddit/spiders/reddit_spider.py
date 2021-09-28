import scrapy
from time import time, sleep

from ..items import RedditItem


class RedditSpider(scrapy.Spider):
    name = 'reddit'
    tag_input = input("Kindly Enter subject to search: ")
    sort_by = input("Kindly Select sort by from Relevance/Hot/Top/New/Comments: ")
    start_urls = [f'https://www.reddit.com/search/?q={tag_input}&sort={sort_by}&type=link']

    def parse(self, response):
        links = response.css("a._2qww3J5KKzsD7e5DO0BvvU::attr(href)")
        yield from response.follow_all(links, callback=self.parse_data)

    def parse_data(self, response):
        reddit_post = {}
        subject_input = self.tag_input
        title = response.css("h1._eYtD2XCVieq6emjKBH3m::text").get()
        votes = response.css("div._1rZYMD_4xY3gRcSS3p8ODO::text").get()
        description = response.css("div._1Ap4F5maDtT1E1YuCiaO0r").get()
        user_links = response.css(".hciOr5UGrnYrZxB11tX9s").css("._3GfQMgsm3HGd3838lwqCST::attr(href)").getall()
        if not user_links:
            user_links = response.css(".hciOr5UGrnYrZxB11tX9s").css(".oQctV4n0yUb0uiHDdGnmE::attr(href)").getall()
            if not user_links:
                user_links = response.css(".hciOr5UGrnYrZxB11tX9s").css("._23wugcdiaj44hdfugIAlnX::attr(href)").getall()

        # user_links = response.css(".DjcdNGtVXPcxG0yiFXIoZ , ._3uanAEshks-qbTwm3q3dOw::attr(href)").getall()

        if description:
            description = "".join(response.css("div._1Ap4F5maDtT1E1YuCiaO0r").css("p::text").getall())
        else:
            description = response.css("._1jNPl3YUk6zbpLWdjaJT1r::text").get()
            if not description:
                img_sec = response.css("div._3Oa0THmZ3f5iZXAQ0hBJ0k").get()
                if img_sec:
                    description = response.css("div._3Oa0THmZ3f5iZXAQ0hBJ0k a::attr(href)").get()
                else:
                    description = response.css('.tErWI93xEKrI2OkozPs7J source::attr(src)').get()
                    if not description:
                        description = "There is no description."
        try:
            num_comments = response.css("span.FHCV02u6Cp2zYL0fhQPsO::text").extract_first().split(' ')[0]
        except ValueError:
            print("Its a Value Error.")
        post_id = response.css("div.Post::attr(id)").get()
        time = response.css("a._3jOxDPIQ0KaOWpzvSQo-1s::text").get()
        post_url = response.url
        try:
            community_name = "/".join(post_url.split('/')[3:5])
        except ValueError:
            print("Its a Value Error.")
        reddit_post = {'domain': subject_input, 'title': title , 'post_id': post_id , 'votes': votes,
                    'num_comments': num_comments, 'description' : description, 'time': time ,  'post_url': post_url,
                    'community_name': community_name}

        parent_arr = {1: []}
        comment_data = []

        comment_section = response.css("div._1YCqQVO-9r-Up6QPB9H6_4")
        # users= response.css('.HZ-cv9q391bm8s7qT54B3')
        # users_list = []

        for levels in comment_section.css("div._3sf33-9rVAO_v4y0pIW_CH"):
            if len(levels.css("div._3tw__eCCe7j-epNCKGXUKk").getall()) != 0:
                comm_id = levels.css("::attr(id)").get()
                if levels.css("div._3tw__eCCe7j-epNCKGXUKk span::text").get() == "level 1":
                    id = comm_id
                    parent_id = 0

                    text = levels.css("div._292iotee39Lmt0MkQZ2hPV").get()
                    if text:
                        text = ''.join(levels.css("div._292iotee39Lmt0MkQZ2hPV").css("p::text").getall())

                    po_id = post_id
                    parent_arr[1].append(id)
                    # user_link = users.css('._3GfQMgsm3HGd3838lwqCST::attr(href)').get()
                    # users_list.append(user_link)
                    record = {
                        'id': id,
                        'parent_id': parent_id,
                        'text': text,
                        'post_id': po_id,
                        # 'user': user_link
                    }

                    comment_data.append(record)

                else:
                    if levels.css("div._3tw__eCCe7j-epNCKGXUKk span::text").get().split(' ')[-1] not in parent_arr:
                        try:
                            l = levels.css("div._3tw__eCCe7j-epNCKGXUKk span::text").get().split(' ')[-1]
                        except ValueError:
                            continue
                        parent_arr[int(l)] = []
                        id = comm_id
                        parent_id = parent_arr[int(l) - 1][-1]

                        text = levels.css("div._292iotee39Lmt0MkQZ2hPV").get()

                        if text:
                            text = ''.join(levels.css("div._292iotee39Lmt0MkQZ2hPV").css("p::text").getall())

                        po_id = post_id
                        parent_arr[int(l)].append(id)
                        # user_link = users.css('._3GfQMgsm3HGd3838lwqCST::attr(href)').get()
                        # users_list.append(user_link)
                        record = {
                            'id': id,
                            'parent_id': parent_id,
                            'text': text,
                            'post_id': po_id,
                            # 'user': user_link
                        }

                        comment_data.append(record)

                    else:
                        try:
                            l = levels.css("div._3tw__eCCe7j-epNCKGXUKk span::text").get().split(' ')[-1]
                        except ValueError:
                            continue
                        id = comm_id
                        parent_id = parent_arr[int(l) - 1][-1]

                        text = levels.css("div._292iotee39Lmt0MkQZ2hPV").get()

                        if text:
                            text = ''.join(levels.css("div._292iotee39Lmt0MkQZ2hPV").css("p::text").getall())

                        po_id = post_id
                        parent_arr[int(l)].append(id)
                        # user_link = users.css('._3GfQMgsm3HGd3838lwqCST::attr(href)').get()
                        # users_list.append(user_link)
                        record = {
                            'id': id,
                            'parent_id': parent_id,
                            'text': text,
                            'post_id': po_id,
                            # 'user': user_link
                        }
                        comment_data.append(record)

        for user in user_links:
            yield response.follow(user, self.parse_user,
                                  meta={'item_post': reddit_post, 'item_comment': comment_data})

    def parse_user(self, response):
        reddit_post = response.meta['item_post']
        comment_data = response.meta['item_comment']

        item = RedditItem()
        # user_class = ['._1LCAhi_8JjayVo7pJ0KIh0::text','._28nEhn86_R1ENZ59eAru8S::text']
        # for user in user_class:
        #     user_name = response.css(f'{user}').extract_first()
        #     if not user_name:
        #         user_name = ''.join(response.css(f'{user}').extract())
        if response.css('._1LCAhi_8JjayVo7pJ0KIh0::text').extract_first():
            user_name = response.css('._1LCAhi_8JjayVo7pJ0KIh0::text').extract_first()
        else:
            user_name = ''.join(response.css('._28nEhn86_R1ENZ59eAru8S::text').extract())

        karma = response.css('#profile--id-card--highlight-tooltip--karma').css(
            '._1hNyZSklmcC7R_IfCUcXmZ::text').extract_first()
        cake_day = response.css('#profile--id-card--highlight-tooltip--cakeday').css(
            '._1hNyZSklmcC7R_IfCUcXmZ::text').extract_first()

        user = {'user_name': user_name, 'karma': karma, 'cake_day': cake_day}

        item['reddit_post'] = reddit_post
        item['comment_data'] = comment_data
        item['user'] = user

        yield item
