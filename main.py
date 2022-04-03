import pandas as pd

from handlers.dispatcher import *


class Parser:
    """Parses links and writes them to DB"""
    def __init__(self):
        with open("registers/links_available.txt", "r") as register:
            self.url_list = register.readlines()

        self.results = []
        self.matches = []
        self.card_dictionary = {}
        self.card_list = []
        self.extract = ""

    def parse_links(self):

        n_url = len(self.url_list)

        status_i = 0
        pipe = r.pipeline()

        for url in self.url_list:
            print(str(url).strip())
            status_i += 1

            print(f"Status: {status_i}/{n_url} links processed")


            extract_try = 0

            while not self.extract:
                html = requests.get(url.strip()).text
                extract_try += 1

                try:

                    if extract_try > 1:
                        print(f"It is my {extract_try} try to get the name of the skin.")
                    elif extract_try > 5:
                        break

                    self.extract = html.split('title="')[1].split('">')[0]
                    print(f"{self.extract}")

                except IndexError as ex:
                    self.extract = ""

                    print(f"Error in extracting the name and exterior of the skin.\n")

            if " | " not in self.extract:
                print(f"ERROR IN GETTING THE NAME OF THE SKIN. RETRIEVED NAME: {self.extract}")
                skin_name = ""
                skin_exterior = ""
            else:
                skin_name = self.extract.split('(')[0]

                skin_exterior = self.extract.split('(')[1].replace(")", "")

            self.extract = ""

            skins_matches = BeautifulSoup(html, 'html.parser').findAll("div",
                                                                       {"class": re.compile(
                                                                           r'item row market_item_\d+')})

            for skin in skins_matches:
                item = str(skin)

                id_ = item.split('data-marketskinid="')[1].split('">')[0].strip()
                name = skin_name
                exterior = skin_exterior
                price = item.split('<div class="price">')[1].split('<')[0].strip()
                img = f'https://lis-skins.ru/market.php?act=request-screenshot&id={id_}'
                link = url
                flt = re.search(r'\s+0\.\d+', item).group(0)

                try:
                    stickers = item.split('<div class="sticker-list">')[1].split('</div>')[0].strip()
                except IndexError as ex:
                    stickers = None

                card_dictionary = {
                    'id': id_.strip(),
                    'name': name.strip(),
                    'exterior': exterior.strip(),
                    'price': price.strip(),
                    'link': link.strip(),
                    'float': flt.strip(),
                    'img': img.strip(),
                    'stickers': str(stickers).strip()}

                self.card_list.append(card_dictionary)

                for key, value in card_dictionary.items():
                    pipe.hset(str(id_), key=str(key), value=str(value))

            if status_i % 100 == 0:
                print(f"100 links processed. Executing pipe...")
                pipe.execute()
                pipe = r.pipeline()
                print(f"Pipe executed! New size of the db is {r.dbsize()}")


def get_all_hashes_from_redis() -> (list, int):

    """GETS ALL HASHES FROM MY REDIS DB"""
    try:
        pipe = r.pipeline()

        for key in r.keys():
            pipe.hgetall(key)

        list_of_rows = pipe.execute()
        status = 200

    except:
        status = 400
        list_of_rows = []
        print(f"Error in getting the values from the DB")

    return list_of_rows, status


def all_available_links_to_file() -> int:

    """GETS ALL OF MY AVAILABLE LINKS TO A TXT FILE"""
    try:
        row_list = get_all_hashes_from_redis()[0]
        df = pd.DataFrame(row_list)

        with open(f"registers/links_available.txt", "w+") as register:
            for unique_link in df.link.unique():

                register.write(f"{unique_link}\n")

        status = 200

    except Exception as ex:
        status = 400
        print(f"Error in printing links from the DB: {ex}")

    return status


async def item_checker() -> int:
    """DELETES ALL KEYS THAT CONTAIN OLD ITEMS FROM A DB"""

    keys_to_delete = []
    list_of_rows_to_check = get_all_hashes_from_redis()[0]

    print(f"Length of rows that I am going to check is {len(list_of_rows_to_check)}")

    df = pd.DataFrame(list_of_rows_to_check)
    num_unique = len(df.name.unique())

    i = 0
    print(f"Going to process keys... Amount of unique names: {num_unique}")
    for unique_name in df.name.unique():
        i += 1
        print(f"Status: {i}/{num_unique}")
        partial_df = df.loc[df['name'] == unique_name]
        all_ids = partial_df['id']
        url = partial_df.iloc[0]['link']
        actual_ids = set()
        res_list = []
        n_try = 0

        while not res_list and not actual_ids:

            n_try += 1

            if n_try > 1:
                print(f"It is my {n_try} try to get the website.")

            try:

                req = requests.get(url).text
                results = BeautifulSoup(req, "html.parser").findAll("div", {"class": re.compile(r'item row market_item_\d+')})
                actual_ids = set(re.findall(r'\d{8}(?=")', str(results)))
                res_list = [id_ for id_ in all_ids if id_ not in actual_ids]

                print(f"name: {unique_name}; keys total: {len(all_ids)}; keys outdated: {len(res_list)}")
                keys_to_delete.append(res_list)

            except Exception as ex:
                print(f"Error in getting the actual ids for the {unique_name}: {ex}")

    keys_to_delete = list(chain.from_iterable(keys_to_delete))
    print(f"Total number of outdated keys: {len(keys_to_delete)}")

    if keys_to_delete:
        pipe = r.pipeline()

        for key in keys_to_delete:
            pipe.delete(key)

        pipe.execute()

    else:
        print("Nothing to delete!")

    return len(keys_to_delete)


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


if __name__ == '__main__':
    '''NOW A TEST ENVIRONMENT'''
    Parser().parse_links()
