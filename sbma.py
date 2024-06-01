import scrapy
import csv

f_names = []
l_names = []

# Open the sbma_IMPORT.csv file, assuming it's in the same directory as your script
with open('sbma_IMPORT.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        f_names.append(row['pfname'])  # Get first names from the 'pfname' column
        l_names.append(row['plname'])  # Get last names from the 'plname' column

# Optional: Print the first few names to verify
print(f"First few first names: {f_names[:5]}")
print(f"First few last names: {l_names[:5]}")


class SbmaSpider(scrapy.Spider):
    name = "sbma"
    headers = {
        'Accept-Language': 'en-US,en;q=0.9',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 OPR/105.0.0.0'
    }

    def start_requests(self):
        for f_name, l_name in zip(f_names[:10], l_names[:10]):
            short_f_name = f_name[:3]  # Use only the first three letters of the first name
            url = f'https://www.certificationmatters.org/find-my-doctor/?dsearch=1&lname={l_name}&fname={short_f_name}&state=&specialty=obstetrics-gynecology'
            yield scrapy.Request(
                url=url,
                callback=self.parse_urls,
                headers=self.headers,
            )

    def parse_urls(self, response):
        rows = response.xpath('//tbody[@class="result-body"]/tr/td[4]')
        for row in rows:
            url = row.xpath('./a/@href').get()
            yield scrapy.Request(
                url=url,
                callback=self.parse_data,
                headers=self.headers,
            )

    def parse_data(self, response):
        response = response.replace(encoding='utf-8')

        head = response.xpath('normalize-space(//h5[@class="line-title"]/strong)').get()
        name = response.xpath('normalize-space(//p[@class="info-cnt doctor-name"]/span[@class="info-label nowrap"])').get()
        degree = response.xpath('normalize-space(//p[@class="info-cnt doctor-education"]/span[@class="info-label nowrap"])').get()
        location = response.xpath('normalize-space(//p[@class="info-cnt doctor-location"]/span[@class="info-label nowrap"])').get()

        certifying_board = response.xpath('normalize-space(//p[@class="info-cnt" and contains(.,"Certifying board:")]/span[@class="info-label nowrap"])').get()
        specialty = response.xpath('normalize-space(//p[@class="info-cnt" and contains(.,"Specialty:")]/span[@class="info-label nowrap"])').get()
        first_certified = response.xpath('normalize-space(//p[@class="info-cnt" and contains(.,"First certified:")]/span[@class="info-label nowrap"])').get()
        most_recent_certification = response.xpath('normalize-space(//p[@class="info-cnt" and contains(.,"Most recent certification:")]/span[@class="info-label nowrap"])').get()
        actively_maintaining_certification = response.xpath('normalize-space(//p[@class="info-cnt" and contains(.,"Actively maintaining certification:")]/span[@class="info-label nowrap"])').get()
        status = response.xpath('normalize-space(//p[@class="info-cnt" and contains(.,"Status:")]/span[@class="info-label nowrap"])').get()

        sub_certifying_board = response.xpath('normalize-space((//p[@class="info-cnt" and contains(.,"Certifying board:")]/span[@class="info-label nowrap"])[2])').get()
        subspecialty = response.xpath('normalize-space(//p[@class="info-cnt" and contains(.,"Subspecialty:")]/span[@class="info-label nowrap"])').get()
        sub_first_certified = response.xpath('normalize-space((//p[@class="info-cnt" and contains(.,"First certified:")]/span[@class="info-label nowrap"])[2])').get()
        sub_most_recent_certification = response.xpath('normalize-space((//p[@class="info-cnt" and contains(.,"Most recent certification:")]/span[@class="info-label nowrap"])[2])').get()
        sub_actively_maintaining_certification = response.xpath('normalize-space((//p[@class="info-cnt" and contains(.,"Actively maintaining certification:")]/span[@class="info-label nowrap"])[2])').get()
        sub_status = response.xpath('normalize-space((//p[@class="info-cnt" and contains(.,"Status:")]/span[@class="info-label nowrap"])[2])').get()

        yield {
            'Head': head,
            'Name': name,
            'Degree': degree,
            'Location': location,
            'Primary Certifying Board': certifying_board,
            'Specialty': specialty,
            'Primary First Certified': first_certified,
            'Primary Actively Maintaining Certification': actively_maintaining_certification,
            'Primary Most Recent Certification': most_recent_certification,
            'Primary Certification Status': status,
            'Subspecialty Certifying Board': sub_certifying_board,
            'Subspecialty': subspecialty,
            'Subspecialty First Certified': sub_first_certified,
            'Subspecialty Actively Maintaining Certification': sub_actively_maintaining_certification,
            'Subspecialty Most Recent Certification': sub_most_recent_certification,
            'Subspecialty Certification Status': sub_status,
            'URL': response.url,
        }
