import time


from handlers.dispatcher import *


class Parser:

    def __init__(self):
        self.max_price = 2000
        self.url = f'https://lis-skins.ru/market/csgo/?sortby=date_desc&price_to={self.max_price}&exterior=2%2C4'
        self.results = []

    def get_html(self):
        html = requests.get(self.url).text
        file_name = 'html files/csgo.html'
        with open(file_name, 'w+') as file:
            soup = BeautifulSoup(html, 'html.parser')
            file.write(soup.prettify())
        return file_name

    def parse_html(self, file_name):
        with open(file_name, 'r') as file:
            html = file.read()
            soup = BeautifulSoup(html, 'html.parser')
        self.results = list(soup.findAll('div', {'class': re.compile(r'item market_item_\d+')}))
        if self.results:
            print('Skins found!')
        else:
            print('Error in getting the contents of website.')
        return self.results


def create_card_list(soup):
    card_dictionary = {}
    card_list = []

    for item in soup:
        item = str(item)

        id_ = item.split('data-marketskinid="')[1].split('">')[0].strip()
        name = item.split('<div class="name-inner">')[1].split('</div>')[0].strip()
        exterior = item.split('<div class="name-exterior">')[1].split('</div>')[0].strip()
        price = item.split('<div class="price">')[1].split('<')[0].strip()
        img = item.split('class="image" src="')[1].split('" title=')[0].strip()
        link = item.split('<a class="name" href="')[1].split('">')[0].strip()
        flt = re.search(r'\s+0\.\d+', item).group(0)

        try:
            stickers = item.split('<div class="sticker-list">')[1].split('</div>')[0].strip()
        except IndexError as ex:
            stickers = None

        card_dictionary = {
            'id': id_,
            'name': name,
            'exterior': exterior,
            'price': price,
            'img': img,
            'link': link,
            'float': flt.strip(),
            'stickers': stickers}

        card_list.append(card_dictionary)

    return card_list


def create_cards(cards_list):
    print(f"Going to process values and create cards... Please wait...")
    for card in cards_list:

        url = card['img']

        logger.info(f'Processing skin {card["name"]}')
        response = requests.get(url)
        with Image.open(BytesIO(response.content)) as img:
            im = Image.new('RGB', (450, 250), (249, 248, 208))
            draw = ImageDraw.Draw(im)
            im.paste(img, (50, 50), mask=img)

        if card['stickers']:
            urls = re.findall(r'https\S+(?=")', card['stickers'])

            i = 0

            for url in urls:
                response = requests.get(url)
                with Image.open(BytesIO(response.content)) as icon:
                    icon = icon.resize((55, 55), Image.ANTIALIAS)
                    im.paste(icon, (200 + 60 * i, 15), mask=icon)
                    i += 1
                    time.sleep(2)

        font = ImageFont.truetype('fonts/Anonymous-Pro/Anonymous_Pro.ttf', size=18)
        font_color = (37, 36, 19)
        draw.text((10, 5), card['name'], font=font, fill=font_color)
        draw.text((10, 20), f"EXT: {card['exterior']}", font=font, fill=font_color)
        draw.text((10, 35), f"Price: {card['price']}", font=font, fill=font_color)
        draw.text((10, 50), f"Float: {card['float']}", font=font, fill=font_color)
        os.makedirs(f"csgocards/{card['name'].split('|')[0].strip()}", exist_ok=True)
        im.save(fp=f"csgocards/{card['name'].split('|')[0].strip()}/{card['id']}.jpg", format='JPEG',
                quality=100, subsampling=0)


def write_to_db(list_of_dictionaries):
    for dictionary in list_of_dictionaries:
        id = dictionary['id']
        for key, value in dictionary.items():
            r.hset(str(id), key=str(key), value=str(value))


def db_to_df():
    list_of_rows = []

    for key in r.keys():
        list_of_rows.append(r.hgetall(key))

    df = pd.DataFrame(list_of_rows)

    # with pd.ExcelWriter('csgocards/register_total.xlsx') as writer:
    #     df.to_excel(writer)
    df['float'] = df['float'].str.strip()
    df['float'] = df['float'].astype(str).replace(' ', '')
    df['float'] = df['float'].astype(float)
    df['price'] = df['price'].str.replace(' ', '')
    df['price'] = df['price'].astype(float)
    df['id'] = df['id'].astype(int)
    return df


def cs_go_main_function(sleep_time=300, to_create_cards=True):
    """Parses the lis-skins website and adds it to the DB"""
    while True:

        try:
            filename = Parser().get_html()
            cards = Parser().parse_html(file_name=filename)
            logger.info(f'Parsed html!')

            card_list = create_card_list(soup=cards)
            logger.info(f'Created card list. Number of cards is {len(card_list)}')

            write_to_db(list_of_dictionaries=card_list)
            logger.info(f'Wrote to db!')

            if to_create_cards:
                create_cards(cards_list=card_list)

            logger.info(f'Done! Size of DB: {len(r.keys())} Going to sleep now... zzz...')
            print(f"Finish! Size of DB: {len(r.keys())}")

        except Exception as ex:
            logger.error(f'Error in cs_go script. {ex}')
            print(f'Error in cs_go script. {ex}')

        finally:
            time.sleep(sleep_time)


if __name__ == '__main__':
    cs_go_main_function()

