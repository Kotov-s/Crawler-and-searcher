import sqlite3

database_name = "db.db"

def calculatePageRank(dbName, iterations=5):
    conn = sqlite3.connect(dbName)
    # Подготовка БД ------------------------------------------
    # стираем текущее содержимое таблицы PageRank
    conn.execute('DROP TABLE IF EXISTS pagerank')
    conn.execute("""CREATE TABLE IF NOT EXISTS pagerank (
                            rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                            urlid INTEGER,
                            score REAL
                        );""")

    # Для некоторых столбцов в таблицах БД укажем команду создания объекта "INDEX" для ускорения поиска в БД
    # Здесь вы создаете индексы для ускорения поиска, что является хорошей практикой.
    conn.execute("DROP INDEX IF EXISTS wordidx;")
    conn.execute("DROP INDEX IF EXISTS urlidx;")
    conn.execute("DROP INDEX IF EXISTS wordurlidx;")
    conn.execute("DROP INDEX IF EXISTS urltoidx;")
    conn.execute("DROP INDEX IF EXISTS urlfromidx;")
    conn.execute('CREATE INDEX IF NOT EXISTS wordidx ON wordlist(word)')
    conn.execute('CREATE INDEX IF NOT EXISTS urlidx ON urllist(url)')
    conn.execute('CREATE INDEX IF NOT EXISTS wordurlidx ON wordlocation(wordid)')
    conn.execute('CREATE INDEX IF NOT EXISTS urltoidx ON linkbetwenURL(fk_ToURL)')
    conn.execute('CREATE INDEX IF NOT EXISTS urlfromidx ON linkbetwenURL(fk_FromURL)')
    conn.execute("DROP INDEX IF EXISTS rankurlididx;")
    conn.execute('CREATE INDEX IF NOT EXISTS rankurlididx ON pagerank(urlid)')
    conn.execute("REINDEX wordidx;")
    conn.execute("REINDEX urlidx;")
    conn.execute("REINDEX wordurlidx;")
    conn.execute("REINDEX urltoidx;")
    conn.execute("REINDEX urlfromidx;")
    conn.execute("REINDEX rankurlididx;")

    # в начальный момент ранг для каждого URL равен 1
    conn.execute('INSERT INTO pagerank (urlid, score) SELECT rowid, 1.0 FROM urllist')
    conn.commit()

    # Цикл Вычисление PageRank в несколько итераций
    for i in range(iterations):
        print("Итерация %d" % (i))

        # Цикл для обхода каждого urlid адреса в urllist БД
        for urlid in conn.execute("SELECT rowid FROM urllist"):

            # назначить коэффициент pr = 0.15
            pr = 1 - 0.85

            # В цикле обходим все страницы, ссылающиеся на данную urlid
            for linking_page_id in conn.execute(f"SELECT fk_FromURL FROM linkbetwenURL WHERE fk_ToURL = {urlid[0]}"):
                linking_page_id = linking_page_id[0]

                # Находим ранг ссылающейся страницы linkingpr.
                linking_score = conn.execute("SELECT score FROM pagerank WHERE urlid = ?", (linking_page_id,)).fetchone()[0]

                # Находим общее число ссылок на ссылающейся странице linkingcount.
                linking_count = conn.execute("SELECT count(*) FROM linkbetwenURL WHERE fk_FromURL = ?", (linking_page_id,)).fetchone()[0]

                # Придавить к pr вычисленный результат для текущего узла
                pr += 0.85 * (linking_score / linking_count)

            # выполнить SQL-запрос для обновления значения score в таблице pagerank БД
            conn.execute('UPDATE pagerank SET score=? WHERE urlid=?', (pr, urlid[0]))

        conn.commit()

calculatePageRank(database_name)
