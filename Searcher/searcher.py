import sqlite3
from prettytable import PrettyTable
from createhtml import createMarkedHtmlFile

database_name = "db.db"

class Searcher:
    def __init__(self, dbName):
        self.conn = sqlite3.connect(dbName)
        
    def __del__(self):
        # закрыть соединение с БД
        self.conn.close()

    def get_word_ids(self, word1, word2):
        cursor = self.conn.cursor()

        query = """
        SELECT rowid FROM wordlist
        WHERE word IN (?, ?)
        """
        cursor.execute(query, (word1, word2))
        res = cursor.fetchall()
        if len(res) < 2:
            raise ValueError("Одно или оба слова не найдены в базе данных.")
        return res[0][0], res[1][0]

    def get_match_rows(self, word1_id, word2_id):
        cursor = self.conn.cursor()
        # Создаем временные таблицы, для того чтобы потом создать запрос из них
        cursor.execute(f"CREATE TEMPORARY TABLE IF NOT EXISTS temp_word1 AS SELECT urlid, location FROM wordlocation WHERE wordid = {word1_id};")
        cursor.execute(f"CREATE TEMPORARY TABLE IF NOT EXISTS temp_word2 AS SELECT urlid, location FROM wordlocation WHERE wordid = {word2_id};")
        cursor.execute("SELECT temp_word1.urlid, temp_word1.location, temp_word2.location FROM temp_word1 JOIN temp_word2 ON temp_word1.urlid = temp_word2.urlid;")
        return cursor.fetchall()


    def search(self, words):
        words = words.split(' ')
        if len(words) != 2:
            raise ValueError("Query should consist of exactly 2 words")      
        word_ids = self.get_word_ids(words[0], words[1])
        match_rows = self.get_match_rows(word_ids[0], word_ids[1])
        return match_rows, word_ids

    def getUrlName(self, urlid):
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT url FROM urllist WHERE rowid = {urlid}")
        return cursor.fetchone()[0]

    def normalizeScores(self, scores, smallIsBetter=False): 
        resultDict = dict() # словарь с результатом
        vsmall = 0.00001  # создать переменную vsmall - малая величина, вместо деления на 0
        minscore = min(scores.values())  # получить минимум
        maxscore = max(scores.values())  # получить максимум

        # перебор каждой пары ключ значение

        if smallIsBetter:
            for (key, val) in scores.items():
                resultDict[key] = float(minscore) / max(vsmall, val)
        else:
            for (key, val) in scores.items():
                # Режим БОЛЬШЕ  вх. значение => ЛУЧШЕ вычислить макс и разделить каждое на макс
                resultDict[key] = float(val) / maxscore
        return resultDict
    
    def dict_of_locations(self, rowsLoc):
        thisdict = {}
        for urlid, wordlocation1, wordlocation2 in rowsLoc:
            if urlid not in thisdict:
                thisdict[urlid] = ({wordlocation1}, {wordlocation2})
            else:
                thisdict[urlid][0].add(wordlocation1)
                thisdict[urlid][1].add(wordlocation2)
        return thisdict

    def frequencyScore(self, rowsLoc):
        countsDict = self.dict_of_locations(rowsLoc)
        for urlid in countsDict:
            countsDict[urlid] = len(countsDict[urlid][0]) * len(countsDict[urlid][1])
        return self.normalizeScores(countsDict, smallIsBetter=False)

    def getSortedList(self, queryString):
        """
        На поисковый запрос формирует список URL, вычисляет ранги, выводит в отсортированном порядке
        :param queryString:  поисковый запрос
        :return:s
        """       
        words = queryString.split(" ")
        # rowsLoc - Список вхождений: urlId, loc_q1, loc_q2, .. слов из поискового запроса "q1 q2 ..."
        # wordids - Список wordids.rowid слов поискового запроса
        rowsLoc, _ = self.search(queryString)

        # Получить m1Scores - словарь {id URL страниц где встретились искомые слова: вычисленный нормализованный РАНГ}
        # как результат вычисления одной из метрик
        m1Scores = self.frequencyScore(rowsLoc)
        #Создать список для последующей сортировки рангов и url-адресов

        rankedScoresList = list()
        for url, score in m1Scores.items():
            pair = (score, url)
            rankedScoresList.append( pair )

        # Сортировка из словаря по убыванию
        # rankedScoresList.sort(reverse=True)

        unique_urls = []
        for id, _, _ in rowsLoc:
            if id in unique_urls:
                continue
            else:
                unique_urls.append(id)
        print(f"Количество уникальных адресов со словами из поиска: {len(unique_urls)}")
        
        print("Табл. 1")
        result_table1 = PrettyTable(["URL id", f"location '{words[0]}'", f"location '{words[1]}'"])
        for i in range(20):
            result_table1.add_row([rowsLoc[i][0], rowsLoc[i][1], rowsLoc[i][2]])
        print(result_table1)

        # Вывод первых N Результатов
        print("Табл. 2")

        m2Scores = self.pagerankScore(unique_urls)

        full_dict = {}
        for score, urlid in rankedScoresList:
            full_dict[urlid] = (score, m2Scores[urlid], (score+m2Scores[urlid])/2) 
        full_dict = dict(sorted(full_dict.items(), key=lambda item: item[1][2], reverse=True))

        result_table = PrettyTable(["Score M1", "Score M2", "Score M3", "URL id", "URL name"])
        result_table.align["URL name"] = "l"
        for urlid, (score1, score2, score3) in list(full_dict.items())[:10]:
            formated_score1 = f"{score1:.2f}"
            formated_score2 = f"{score2:.2f}"
            formated_score3 = f"{score3:.2f}"
            url_name = self.getUrlName(urlid)
            result_table.add_row([formated_score1, formated_score2, formated_score3, urlid, url_name])
        print(result_table)

        #  Создаем 3 странички
        for i in range(3):
            createMarkedHtmlFile(f"html/{i+1}.html", self.get_page_words(rankedScoresList[i]), words)
            print(f"- Страничка html/{i+1}.html создана")

    def pagerankScore(self, rows):
        # Инициализация словаря для хранения результата PageRank
        pagerank_scores = {}

        # Получение значений PageRank из базы данных и нормализация
        for urlid in rows:
            score = self.conn.execute(f"SELECT score FROM pagerank where urlid={urlid}").fetchone()[0]
            pagerank_scores[urlid] = score

        return self.normalizeScores(pagerank_scores, smallIsBetter=False)

    def get_page_words(self, id):        
        sql = f"select word from wordlist join wordlocation on wordid=wordlist.rowid where urlid={id[1]};"
        result = self.conn.execute(sql).fetchall()
        words = [row[0] for row in result]
        return words

# Другие метрики

    def __locationScore(self, rowsLoc):
        locationsDict = {}
        # Задаем по-умолчанию значение 1000000
        for urlid, _, _ in rowsLoc:
            if urlid not in locationsDict:
                locationsDict[urlid] = 1000000
        thisdict = self.dict_of_locations(rowsLoc)
        for urlild in thisdict:
            locationsDict[urlild] = min(thisdict[urlild][0]) + min(thisdict[urlild][1])
        return self.normalizeScores(locationsDict, smallIsBetter=True)

    def __distanceScore(self, rowsLoc):
        # Создать mindistanceDict - словарь с дистанций между словами внутри комбинаций искомых слов
        mindistanceDict = {}
        
        if len(rowsLoc[0]) == 2:
            for urlid, _ in rowsLoc:
                if urlid not in mindistanceDict:
                    mindistanceDict[urlid] = 1
            return mindistanceDict
        else:
            for urlid, (loc_q1, loc_q2) in self.dict_of_locations(rowsLoc).items():
                # По умолчанию заполняем 1000000
                min_diff = 1000000
                for i in loc_q1:
                    for j in loc_q2:
                        diff = abs(i - j)
                        if diff < min_diff:
                            min_diff = diff
                mindistanceDict[urlid] = min_diff
        return self.normalizeScores(mindistanceDict, smallIsBetter=True)
    

def main():
    search = Searcher(database_name)
    search.getSortedList("Миа Гот")

if __name__ == "__main__":
    main()