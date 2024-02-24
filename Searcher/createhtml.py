def createMarkedHtmlFile(markedHTMLFilename, testText, testQueryList ):
    #Приобразование текста к нижнему регистру

    #Получить html-код с маркировкой искомых слов
    htmlCode = getMarkedHTML(testText, testQueryList)
    
    #сохранить html-код в файл с указанным именем
    file = open(markedHTMLFilename, 'w', encoding="utf-8")
    file.write(htmlCode)
    file.close()


def getMarkedHTML(wordList, queryList):
    content = """<!DOCTYPE html>
                <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <title>Document</title>
                </head>
                <body>"""

    for word in wordList:
        if word == queryList[0]:
            content += f"<mark>{word}</mark> "
        elif word == queryList[1]:
            content += f'<mark style="background-color: red;">{word}</mark> '
        else:
            content += word + " "
    content +=" </body>"
    return content