import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.selector import Selector
import pandas as pd
from datetime import datetime
import re
import json
from collections import defaultdict
from urllib.parse import urlencode
import requests
import ast
import os

def get_proxies(key):
    res = requests.get("https://proxy.webshare.io/api/proxy/list/", headers={"Authorization": f'Token {key}'})
    proxies = []
    for x in res.json()["results"]:
        proxy = f'{x["username"]}:{x["password"]}@{x["proxy_address"]}:{x["ports"]["http"]}'
        proxies.append(f"https://{proxy}")
    
    return proxies

today = datetime.today()
date = today.strftime("%Y-%m-%d")
output_date = today.strftime("%Y%m%d")

retailer_locale = 'Notino'
retailer_locale_name = 'Notino PL'

class Notino(scrapy.Spider):
   name = "notino"

   base_url = "https://www.notino.pl"
   starting_url = "https://www.notino.pl/api/navigation/navigation/notino.pl"

   headers = {
      'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
      'referer': None,
    }
    
   custom_settings = {
    "CONCURRENT_REQUESTS_PER_DOMAIN": 3,
    "DOWNLOAD_DELAY": .24,
      "DOWNLOADER_MIDDLEWARES": {
          'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
          'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
      },
      "ROTATING_PROXY_LIST": get_proxies(os.getenv('PROXY_KEY')),
      "ROTATING_PROXY_PAGE_RETRY_TIMES": 1, 
      "ROBOTSTXT_OBEY": False,
      #    'LOG_LEVEL': 'INFO',
      'RETRY_TIMES': 6,
      'RETRY_ENABLED': True,
      'DOWNLOAD_FAIL_ON_DATALOSS': False,
      'DOWNLOAD_TIMEOUT': 9,
      'LOG_FILE': "logs.txt"
   }
    
   final_created = False

   def start_requests(self):
        for target_cat in [("55544", "perfumy"), ("3644", "kosmetyka/kosmetyki-podkreslajace"),
                            ("3649", "kosmetyka/kosmetyki-do-wlosow"), ("3645", "kosmetyka/kosmetyki-do-twarzy"), 
                            ("3646", "kosmetyka/kosmetyki-do-ciala"), ("4891", "pielegnacja-zebow"), 
                            ("23235", "kosmetyki-dla-mezczyzn"), ("37171", "dermokosmetyki"),
                            ("54041", "drogeria")]:
                                
            cat_number, href = target_cat[0], target_cat[1]
            page_number = 1
            page = f"{page_number}-3-{cat_number}"
            
            body = f'{{"urlPart":"{href}","pageSize":48,"filterString":"{page}","include":{{"filtration":false,"breadcrumbs":false,"navigationTree":false,"searchCategories":false,"listing":true,"specialPageData":false}}}}'
            yield scrapy.Request(url=self.starting_url, headers=self.api_headers(), method='POST', 
                                meta={"cat_number": cat_number, "href": href},
                                callback=self.parse_brands_api,
                                body=body
                                )
      
   def api_headers(self):
        api_headers = {
          'authority': 'www.notino.pl',
          'accept': 'application/json, text/plain, */*',
          'accept-language': 'en-US,en;q=0.9,ar;q=0.8,en-AU;q=0.7,en-GB;q=0.6,it;q=0.5',
          'cache-control': 'no-cache',
          'content-type': 'application/json',
          'cookie': 'source=direct; source45=direct; lastSource=direct; ab80=1; grd=69608014135110125; npcount=1; db_ui=e517b7c5-d8e8-e864-a961-089ff332b20a; __exponea_etc__=42f1d205-a4b3-46a0-aba2-f5312a3b9216; db_uicd=4540e27b-7616-a0e0-bf0f-b16602472421; lastSource=direct; __exponea_time2__=-0.18779420852661133; modiface-label-listing-show=1; TS01485ffe=016bdf2fdce572af2e2802e562e277d5d2089b082dd67d0eb67f76aa292794098438fda3925f4337473eee865c1c427daa500c1245; USER=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzaG9wIjoibm90aW5vLnBsIiwibGFuZyI6IjQiLCJjdXJyIjoiNSIsImNncnAiOiIyNDciLCJjYXJ0IjoiRDY1NTAwMDAtQTEwQS1ENkIxLTIzRkEtMDhEQURFOUIwQTU5Iiwicm9sZSI6IkFub255bW91cyIsImdyZCI6IjY5NjA4MDE0MTM1MTEwMTI1Iiwic2lkIjoiRDY1NTAwMDAtQTEwQS1ENkIxLTIzRTEtMDhEQURFOUIwQTU5IiwibHRhZyI6InBsLVBMIiwiY2xpZW50Ijoid2ViIiwiaWF0IjoxNjcxMTIyMDI1LCJpc3MiOiJub3Rpbm8ifQ.1xgL5JPlmMJtouKtRawtGVPs7qUPLgDihp4OCyY2dm0; lpv=aHR0cHM6Ly93d3cubm90aW5vLnBsL3BlcmZ1bXkvP2Y9MS0xLTU1NTQ0; TS0178d2ea=016bdf2fdc24d13bb5717ad25ba908854ffa9beea2d67d0eb67f76aa292794098438fda3926f17934966829ea6694d0a0bc2a0be010ab091102e867f2d098c4b9eba849a3afe51b8ed37afc1ed58a1d489d6e114ebc3f6a00a9f97ae8a8803294439f4fb8026a9cc0000535fbbc0e9ef9b7356ec70914572a7f7fa80b185ac530df5e8dff0; TS01adc846=016bdf2fdc117d25b45dce52712eb50d3d24e450f2d67d0eb67f76aa292794098438fda392ceba151c001ae1cc62f5a64139dc64531c9c40b1c10a3083b39e638b3cc952d0; TS4ff28536027=08a5d12542ab20006f40ec8d84af4f559cf0274680e7fbd54dba5f448ba8d3977966f4605b511446081d9a3be41130009445813f6f41a681bd92634c56d62c19e1777de9a7e29e2a16d3c69d7576bddc612ca92aa506a3843588b019a1d51644',
          'origin': 'https://www.notino.pl',
          'pragma': 'no-cache',
          'referer': None,
          'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
          'sec-ch-ua-mobile': '?0',
          'sec-ch-ua-platform': '"Windows"',
          'sec-fetch-dest': 'empty',
          'sec-fetch-mode': 'cors',
          'sec-fetch-site': 'same-origin',
          'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
        }
        return api_headers


   def parse_brands_api(self, response):
        json_response = json.loads(response.text)
        href = response.meta.get("href")
        cat_number = response.meta.get("cat_number")
        

        for p in json_response['listing']['products']:
            url = self.base_url + p['url']
            yield scrapy.Request(url=url, headers=self.headers, callback=self.parse_details)
                                
        current_page = json_response['listing']['currentPage']
        number_of_pages = json_response['listing']['numberOfPages']
        
        if number_of_pages > 0 and current_page != number_of_pages:
            page_number = current_page + 1
            page = f"{page_number}-3-{cat_number}"
            
            body = f'{{"urlPart":"{href}","pageSize":48,"filterString":"{page}","include":{{"filtration":false,"breadcrumbs":false,"navigationTree":false,"searchCategories":false,"listing":true,"specialPageData":false}}}}'

            yield scrapy.Request(url=self.starting_url, headers=self.api_headers(), method='POST', body=body, 
                                 callback=self.parse_brands_api,
                                 meta={"href": href, "cat_number": cat_number})
                                 

   def get_size(self, text):
        size = ""
        find_size = re.search(r"\d+[.,]?\d*\s?(ml(\s|$)|fl\.\s?oz|g(\s|$)|oz\.|kg(\s|$)|szt\.)", text, flags=re.IGNORECASE)
        if find_size:
            size = find_size[0]
        return size

   def parse_details(self, response):
        text = response.text
            
        tags = ""
        active_ingreds = ""
        
        try:
            primary_name = response.css("span.sc-3sotvb-4 *::text").get().strip()
        except:
            primary_name = ""
            
        try:
            secondary_name = response.css("span.sc-3sotvb-5 *::text").get().strip()
        except:
            secondary_name = ""
            
        product_name = f"{primary_name} {secondary_name}"
        
        product_url = re.split(r"\/p\-\d+", response.request.url)[0]
        req_url = product_url.split("/")
        req_url = [ i  for i in req_url if i.strip() != '']
        brand_name = req_url[-2].replace("-", " ").replace("_", " ").title()

        
        try:
            ratings_params = re.findall(r"(?<=\"aggregateRating\"\:).*?(?=\})", text)[0]
            rating = re.findall(r"(?<=\"ratingValue\"\:).*?(?=\,)", ratings_params)[0]
            num_reviews = re.findall(r"(?<=\"ratingCount\"\:).*?$", ratings_params)[0]
        except:
            rating = ""
            num_reviews = ""
            
        unit = ""
        backup_size = ""
        final = []
        
        item = {
            "retailer_locale": retailer_locale,
            "retailer_locale_name": retailer_locale_name,
            "Brand Name": brand_name if brand_name else "N/A", 
            "product_name": product_name.strip().replace("=", "") if product_name else "N/A",
            "product_url": product_url,
            "Descriptions": "N/A",
            "Full Ingredients": "N/A",
            "Key Ingredients": active_ingreds if active_ingreds else "N/A",
            "Price": "N/A",
            "Sale": "N",
            "Sale Price": "N/A",
            "Sold Out?": "N/A",
            "Product Size": "N/A",
            "Variant Name": "N/A",
            "num_reviews": num_reviews if num_reviews else "N/A",
            "star_rating": rating if rating else "N/A",
            "tags": tags if tags else "N/A",
            "scrape_date": date,
        }  
        try:
            b = "".join(response.css("div#pdSelectedVariant div.ihLyFa span *::text").extract()).strip()
            splitted = b.split(" ")
            if len(splitted) > 1:
                unit = splitted[-1]
            backup_size = re.search("\d+[.]?\d*", b)[0]
        except:
            pass
        
        if response.css('li[data-testid^="color-picker-item"]'):
            variants = response.css('li[data-testid^="color-picker-item"]')
            var_type = "Color"
            variant_ids = [var.css('a[id^="pd-variant-"]::attr(id)').get().split("-")[-1] for var in variants]
        elif response.css('div#pdVariantsTile ul li'):
            variants = response.css('div#pdVariantsTile ul li')
            var_type = "Size"
            variant_ids = [var.css('a[id^="pd-variant-"]::attr(id)').get().split("-")[-1] for var in variants]
        else:
            variant_ids = [response.css("input[name='productId']::attr(value)").get()]
            variant_ids = list(filter(None, variant_ids))
            var_type = ""
            if not variant_ids:
                variant_ids = [re.findall(r"(?<=productId\=)\d+(?=&)", text)[0]]

        for var in variant_ids:
            current_item = item.copy()

            try:
                unit
            except:
                unit = ""
                
            sale_price = "N/A"
            
            json_select = re.findall(fr"(?<=Variant:{var}\":).*\"primaryCategories\".*?}}}}", text, flags=re.MULTILINE|re.DOTALL)[0]

            variant_name = re.findall(r"(?<=\"additionalInfo\"\:\").*?(?=\")", json_select, flags=re.MULTILINE|re.DOTALL)[0]
            sold_out = "Y" if re.findall(r"(?<=\"canBuy\"\:).*?(?=\,)", json_select, flags=re.MULTILINE|re.DOTALL)[0] == "false" else "N"
            price_pars = re.findall(r"(?<=\"price\"\:\{\").*?(?=\},)", json_select, flags=re.MULTILINE|re.DOTALL)[0]
            normal_price = re.search(r"(?<=\"value\"\:).*?(?=\,)", price_pars)[0]
            
            try:
                ingreds = re.findall(r"(?<=\"ingredients\"\:).*?(?=\,\")", json_select)[0].strip()
            except:
                ingreds = ""
            
            try:
                desc = re.findall(r"(?<=\"description\"\:).*?(?=\,\")", text)[0]
            except:
                desc = ""
            try:
                characteristics = re.findall(r"(?<=characteristics\"\:).*?(?=\}\]\,)", json_select)[0]
                labels = re.findall(r"(?<=\"name\"\:\").*?(?=\"\,)", characteristics)
                values = re.findall(r"(?<=\"values\"\:\[).*?(?=\])", characteristics)
                characteristics_list = []
                for i in range(len(values)):
                    characteristics_list.append("{}: {}".format(labels[i].strip(), values[i].replace('\"', '').strip()))
                if characteristics_list:
                    desc += "\n Characteristics\n " + "\n ".join(characteristics_list)
            except:
                pass
        
            desc = re.sub('\n+', '\n ', re.sub(r"<.*?\>", "\n", desc.replace("&gt;", ">").replace("&lt;", "<").replace("\"", "")).strip())            

            try:
                original_price_pars = re.findall(r"(?<=\"originalPrice\"\:\{\").*?(?=\},)", json_select, flags=re.MULTILINE|re.DOTALL)[0]
                original_price = re.search(r"(?<=\"value\"\:).*?(?=\,)", original_price_pars)[0]
            except:
                original_price = "N/A"
        
            if original_price != "N/A":
                if original_price == normal_price:
                    price = original_price
                    sale_price = "N/A"
                else:
                    price = original_price
                    sale_price = normal_price
            else:
                price = normal_price
                sale_price = "N/A"
        
            current_item["Price"] = price
            current_item["Sale Price"] = sale_price
            current_item["Sold Out?"] = sold_out
            
            if not var_type:
                find_size = self.get_size(variant_name)
                current_item["Variant Name"] = "N/A"
                current_item["Product Size"] = find_size if find_size else "N/A"
            elif var_type == "Size":
                current_item["Product Size"] = variant_name.strip() if variant_name else "N/A"
            elif var_type == "Color":
                current_item["Variant Name"] = variant_name.strip() if variant_name else "N/A"
            
            if sale_price == "N/A":
                current_item["Sale"] = "N"
            else:
                current_item["Sale"] = "Y"
                
            if ingreds.strip("\"").strip():
                current_item["Full Ingredients"] = ingreds.replace("\"", "").replace("\n", "").replace("\r", "").strip() if ingreds.strip() != "null" else "N/A"
            else:
                current_item["Full Ingredients"] = "N/A"
            
            current_item["Descriptions"] = desc if desc else "N/A"
            
            if unit and unit not in current_item["Product Size"] and current_item["Product Size"] != "N/A":
                current_item["Product Size"] = current_item["Product Size"] + " " + unit
            
            if current_item["Product Size"] == "N/A" and current_item["Variant Name"] != "N/A":
                find_size = self.get_size(current_item["Variant Name"])
                current_item["Product Size"] = find_size.strip() if find_size else "N/A"
            
            if current_item["Product Size"] == "N/A":
                
                current_item["Product Size"] = f"{backup_size} {unit}" if backup_size else "N/A"
            
            if current_item["Variant Name"] == current_item["Product Size"] and current_item["Product Size"] != "N/A":
                current_item["Variant Name"] = "N/A"
            
            current_item["Product Size"] = re.sub(r"(?<=\d)\,", ".", current_item["Product Size"]).strip() if current_item["Product Size"] != "N/A" else "1 szt"
            
            final.append(current_item)
    
        if self.final_created:
            pd.DataFrame(final).to_csv(f"product_details_table_{retailer_locale_name}_{output_date}.csv", 
                                          encoding='utf-8-sig', mode='a', index=False, header=False)
        else:
            self.final_created = True
            pd.DataFrame(final).to_csv(f"product_details_table_{retailer_locale_name}_{output_date}.csv", 
                              encoding='utf-8-sig', mode='w', index=False)

if __name__ == '__main__':
    process = CrawlerProcess()
    process.crawl(Notino)
    process.start()

    details_table = pd.read_csv(f"product_details_table_{retailer_locale_name}_{output_date}.csv", 
                        encoding='utf-8-sig', na_filter = False)
    details_table = details_table.sort_values(by=['product_name'])
    details_table.to_csv(f"product_details_table_{retailer_locale_name}_{output_date}.csv", 
                                          encoding='utf-8-sig', mode='w', index=False)
    
    
    list_table = details_table.drop_duplicates(subset='product_url', keep="first")
    list_table = list_table[['retailer_locale', 'retailer_locale_name',
                    'Brand Name', 'product_name', 'product_url',
                    'scrape_date']].rename({'Brand Name': 'brand_name'}, axis=1)
                    
    list_table.to_csv(f"product_list_table_{retailer_locale_name}_{output_date}.csv", 
                 encoding='utf-8-sig', index=False)
