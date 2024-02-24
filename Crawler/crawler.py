import sqlite3
import requests
import bs4
import datetime


class simpleCrawler:

    # создание БД
    def __init__(self, fileName):
        self.connection = sqlite3.connect(fileName)
        self.previousPages = []
        print("База создана", fileName)
        
    # корректное завершение
    def __del__(self):
        self.connection.close()
        pass

    # создать таблицы в БД
    def initDB(self):
        print("Создание таблиц")
        cursor = self.connection.cursor()

        # 1. WordList ------------------------------------------------------------------
        sqlDropWordList = """ DROP TABLE   IF EXISTS    wordlist; """
        print(sqlDropWordList)
        cursor.execute(sqlDropWordList)

        sqlCreateWordList = """ CREATE TABLE   IF NOT EXISTS    wordlist (
                                rowid INTEGER   PRIMARY KEY   AUTOINCREMENT,
                                word TEXT,
                                isFiltred INTEGER
                           ); 
       """
        print(sqlCreateWordList)
        cursor.execute(sqlCreateWordList)

        # 2. UrlList ------------------------------------------------------------------
        sqlDropUrlList = """ DROP TABLE   IF EXISTS    urllist; """
        print(sqlDropUrlList)
        cursor.execute(sqlDropUrlList)

        sqlCreateURLList = """ CREATE TABLE   IF NOT EXISTS    urllist (
                                rowid INTEGER   PRIMARY KEY   AUTOINCREMENT,
                                url TEXT
                           ); 
       """
        print(sqlCreateURLList)
        cursor.execute(sqlCreateURLList)

        # 3. WordLocation ------------------------------------------------------------------
        sqlDropWordLocaton = """ DROP TABLE   IF EXISTS    wordlocation; """
        print(sqlDropWordLocaton)
        cursor.execute(sqlDropWordLocaton)

        sqlCreateWordLocaton = """ CREATE TABLE   IF NOT EXISTS    wordlocation (
                                rowid INTEGER   PRIMARY KEY   AUTOINCREMENT,
                                wordid INTEGER,
                                urlid INTEGER,
                                location INTEGER
                           ); 
       """

        print(sqlCreateWordLocaton)
        cursor.execute(sqlCreateWordLocaton)

        # 4. linkBetwenURL ------------------------------------------------------------------
        sqlDropLinkBetwenURL = """ DROP TABLE   IF EXISTS    linkBetwenURL; """
        print(sqlDropLinkBetwenURL)
        cursor.execute(sqlDropLinkBetwenURL)

        sqlCreateLinkBetwenURL = """ CREATE TABLE   IF NOT EXISTS    linkbetwenURL (
                                rowid INTEGER   PRIMARY KEY   AUTOINCREMENT,
                                fk_FromURL INTEGER,
                                fk_ToURL INTEGER
                           ); 
       """

        print(sqlCreateLinkBetwenURL)
        cursor.execute(sqlCreateLinkBetwenURL)

        # 5. linkWord ------------------------------------------------------------------
        sqlDropLinkWord = """ DROP TABLE   IF EXISTS    linkWord; """
        print(sqlDropLinkWord)
        cursor.execute(sqlDropLinkWord)

        sqlCreateLinkWord = """ CREATE TABLE   IF NOT EXISTS    linkword (
                                rowid INTEGER   PRIMARY KEY   AUTOINCREMENT,
                                fk_word_id INTEGER,
                                fk_link_id INTEGER
                           ); 
       """

        print(sqlCreateLinkWord)
        cursor.execute(sqlCreateLinkWord)

    # обходит список страниц, глубина обхода
    def crawl(self, pageList, depth=2):
        self.previousPages = pageList

        print("Обход страниц")

        nextPagesSet = set()

        for i in range(0, depth):
            print("Глубина = ", i)

            counter = 0
            for pageURL in pageList:

                try:
                    print(
                        f"{counter}/{len(pageList)} {datetime.datetime.now().time()} - 6. crawl - Попытка открыть страницу... ", end="")

                    # 1. Запрос HTML-кода по указанному URL
                    html_doc = requests.get(pageURL).text

                except Exception as e:
                    # 1.1 Что делать, если страница не доступна
                    print(e)
                    print("Не удалось открыть страницу")
                    continue

                # 2. Разобрать HTML-кода парсером на составные элементы
                soup = bs4.BeautifulSoup(html_doc, "html.parser")
                counter += 1

                # 3. Содержимое добавить в индекс
                self.addToIndex(soup, pageURL)
                # 4. Извлечь все ссылки на внешние страницы
                linksList = soup.find_all('a')

                cursor = self.connection.cursor()
                # link['href']
                for link in linksList:

                    if 'href' in link.attrs:
                        toURL = link.attrs['href']
                        

                        if toURL[0:4] == 'http' and not self.isIndexed(toURL):
                            nextPagesSet.add(toURL)
                            previousURLID = self.getEntryId("urllist", "url", f'{pageURL}')
                            nextURLID = self.getEntryId("urllist", "url", f'{toURL}')
                            sql_insertIntoLinkBetween = f"insert into linkBetwenURL (fk_fromurl, fk_tourl) values ({previousURLID}, {nextURLID});"
                            cursor.execute(sql_insertIntoLinkBetween)

                            arrOfLinkWords = link.text.split()
                            for linkWord in arrOfLinkWords:
                                sql = f"select rowid from wordlist where word = '{linkWord}';"
                                try:
                                    word_id = cursor.execute(sql).fetchone()
                                except:
                                    continue
                                if word_id == None:     
                                    if linkWord != '1234' or linkWord != '56':
                                        isFiltred = 0
                                    else:
                                        isFiltred = 1
                                    sql = f"insert into wordList (word, isFiltred) VALUES ('{linkWord}', {isFiltred});"
                                    cursor.execute(sql)
                                    word_id = cursor.lastrowid
                                else:
                                    word_id = word_id[0]
                                
                                sql = f"insert into linkword (fk_word_id, fk_link_id) values ({word_id}, {word_id});"
                                cursor.execute(sql)
                    else:
                        continue

                self.connection.commit()
                # Конец обхода одной из страниц ---------------
            pageList = list(nextPagesSet)

    def addToIndex(self, soup, url):
        print("1. Индексирование страницы", url)

        cursor = self.connection.cursor()
        # Проверка, если есть уже значениия в таблице wordlocation
        res = cursor.execute(f'select rowid from wordlocation where urlid = {self.isIndexed(url)};').fetchone()   
        if res:
            # url Страницы уже в БД -> обрабатывать не будет
            print("Уже обработана", self.isIndexed(url))
            return False
        else:
            text = self.getTextOnly(soup)
            wordsList = self.separateWords(text)
            current_urlID = self.getEntryId("urllist", "url", url)
            # Позиция слова
            counter = 0
            for word in wordsList:

                isThere = f"select rowid from wordList where word='{word}';"

                try:
                    word_id = cursor.execute(isThere).fetchone()
                except:
                    counter += 1
                    continue
                
                if word_id == None:
                    if word == '1234' or word == '56':
                        sql = f"insert into wordList (word, isFiltred) VALUES ('{word}', 1);"
                        cursor.execute(sql)
                    else:
                        sql = f"insert into wordList (word, isFiltred) VALUES ('{word}', 0);"
                        cursor.execute(sql)
                    word_id = cursor.lastrowid
                else:
                    word_id = word_id[0]
                sql = f"insert into wordlocation (wordid, urlid, location) VALUES ({word_id}, {current_urlID}, {counter});"
                cursor.execute(sql)

                counter += 1
            
            return True

    def isIndexed(self, toURL):
        # Проверка наличия toURL в БД (urllist)
        cursor = self.connection.cursor()

        sql = "SELECT rowid FROM urllist WHERE url= ?;"
        resultRow = cursor.execute(sql, (toURL,)).fetchone()

        if resultRow == None:
            # искомая строка не нашлась
            return False
        else:
            # искомая строка присутствует
            return resultRow[0]

    def getTextOnly(self, soup):
        return soup.get_text()

    # ---------------------------------------------------
    def separateWords(self, fullText):
        listOWwords = fullText.split()
        return listOWwords

    # ---------------------------------------------------
    def getEntryId(self, tableName, columnName, value):
        cursor = self.connection.cursor()

        sqlSelect = f"SELECT rowid FROM {tableName} WHERE {columnName} = ?"
        resultRowSelect = cursor.execute(sqlSelect, (value,)).fetchone()

        if resultRowSelect == None:
            sqlInsert = f"INSERT INTO {tableName} ({columnName}) VALUES (?)"
            result = cursor.execute(sqlInsert, (value,))
            return result.lastrowid
        else:
            return resultRowSelect[0]


sc1 = simpleCrawler("db.db")
sc1.initDB()


mypageslist = ["https://ru.wikipedia.org/wiki/1234_%D0%B3%D0%BE%D0%B4",
               "https://ru.wikipedia.org/wiki/%D0%93%D0%BE%D1%82,_%D0%9C%D0%B8%D0%B0"]


sc1.crawl(mypageslist, 2)
