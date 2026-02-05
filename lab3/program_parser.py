import requests
from bs4 import BeautifulSoup
import datetime
import pandas as pd
import sqlite3
import numpy


class Parser:
    def __init__(self, db_path='database.db'):

        self.links = []
        self.db_path = db_path
        
    def getDownloadLinks(self, dateFrom, dateTo):

        url = 'https://spimex.com/markets/oil_products/trades/results/?page=page-'
        pagenum = 3
        endFlag = False
        
        while not endFlag:
            try:
                response = requests.get(url + str(pagenum))
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')

                container = soup.find('div', {'data-tabcontent': '1'}, class_='page-content__tabs__block')
                
                if not container:
                    break
                
                found_links = False
                for ahref in container.find_all('a', class_='accordeon-inner__item-title link xls'):
                    filelink = ahref.get('href', '')
                    if not filelink:
                        continue

                    index = filelink.find('.')
                    if index == -1:
                        continue
                    
                    filedate = filelink[index-14:index-6]
                    if len(filedate) != 8:
                        continue
                    
                    try:
                        date = datetime.date(int(filedate[0:4]), int(filedate[4:6]), int(filedate[6:8]))
                    except (ValueError, IndexError):
                        continue

                    if dateTo >= date:
                        if date >= dateFrom:
                            link = ahref['href']
                            index = link.find('?')
                            if index != -1:
                                link = 'https://spimex.com' + link[0:index]
                            else:
                                link = 'https://spimex.com' + link
                            
                            
                            self.links.append(link)

                            try:
                                try:
                                    df = pd.read_excel(link, engine='xlrd')
                                except:
                                    try:
                                        df = pd.read_excel(link, engine='openpyxl')
                                    except:
                                        df = pd.read_excel(link)

                                header_row = None
                                for idx in range(min(10, len(df))):
                                    row_str = ' '.join([str(cell) for cell in df.iloc[idx].values if pd.notna(cell)])
                                    if any(keyword in row_str.lower() for keyword in ['код', 'наименование', 'инструмент']):
                                        header_row = idx
                                        break
                                
                                if header_row is not None:
                                    df = pd.read_excel(link, header=header_row)
                                    if len(df.columns) > 0 and (df.columns[0] == 'Unnamed: 0' or df.columns[0] == 0):
                                        df = df.drop(df.columns[0], axis=1)
                                else:
                                    if len(df.columns) > 0:
                                        df = df.drop(df.columns[0], axis=1)
                                
                                df = df.dropna(how='all')
                                
                                if len(df.columns) >= 14:
                                    df.columns = ['КодИнструмента', 'НаименованиеИнструмента', 'БазисПоставки', 
                                                'ОбъемДоговоровЕИ', 'ОбъемДоговоровРуб',
                                                'ИзмРынРуб', 'ИзмРынПроц', 'МинЦена', 'СреднЦена', 'МаксЦена', 
                                                'РынЦена', 'ЛучшПредложение',
                                                'ЛучшСпрос', 'КоличествоДоговоров']
                                    
                                    df = df.dropna(subset=['КодИнструмента', 'НаименованиеИнструмента'], how='all')
                                    df = df[df['КодИнструмента'].notna()]
                                    df = df[df['КодИнструмента'].astype(str).str.strip() != '']
                                    df['Дата'] = date.strftime('%Y-%m-%d')
                                    df['Товар'] = df['НаименованиеИнструмента'].apply(
                                        lambda x: str(x).split()[0] if pd.notna(x) and str(x).strip() else None
                                    )
                                    df = df.replace('-', numpy.nan)

                                    conn = sqlite3.connect(self.db_path)
                                    df.to_sql('Table', conn, if_exists='append', index=False)
                                    conn.close()
                                    
                                    print(f"Загружено {len(df)} записей за {date}")
                                    found_links = True
                                else:
                                    print(f"Неверное количество колонок в файле: {len(df.columns)}, ожидается 14")
                                
                            except Exception as e:
                                print(f"Ошибка при обработке файла {link}: {e}")
                                import traceback
                                traceback.print_exc()
                                continue
                        else:
                            endFlag = True
                            break
                    else:

                        continue

                if not found_links and pagenum > 3:
                    break
                
                pagenum += 1
                
            except requests.exceptions.RequestException as e:
                print(f"Ошибка при загрузке страницы {pagenum}: {e}")
                break
            except Exception as e:
                print(f"Ошибка при обработке страницы {pagenum}: {e}")
                break
        
        return self.links
