"""
Модуль для парсинга данных с сайта spimex.com
Основан на примере из папки "Пример"
"""
import requests
from bs4 import BeautifulSoup
import datetime
import pandas as pd
import sqlite3
import numpy


class Parser:
    def __init__(self, db_path='database.db'):
        """Инициализация парсера"""
        self.links = []
        self.db_path = db_path
        
    def getDownloadLinks(self, dateFrom, dateTo):
        """
        Получает ссылки на Excel файлы с сайта и загружает данные в базу
        
        Args:
            dateFrom: Дата начала периода (datetime.date)
            dateTo: Дата конца периода (datetime.date)
        
        Returns:
            list: Список ссылок на скачанные файлы
        """
        url = 'https://spimex.com/markets/oil_products/trades/results/?page=page-'
        pagenum = 3
        endFlag = False
        
        while not endFlag:
            try:
                response = requests.get(url + str(pagenum))
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Ищем контейнер с данными (как в примере)
                container = soup.find('div', {'data-tabcontent': '1'}, class_='page-content__tabs__block')
                
                if not container:
                    print(f"Контейнер не найден на странице {pagenum}, останавливаемся")
                    break
                
                # Ищем все ссылки на Excel файлы
                found_links = False
                for ahref in container.find_all('a', class_='accordeon-inner__item-title link xls'):
                    filelink = ahref.get('href', '')
                    if not filelink:
                        continue
                    
                    # Извлекаем дату из ссылки
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
                    
                    # Проверяем, попадает ли дата в диапазон
                    if dateTo >= date:
                        if date >= dateFrom:
                            # Формируем полную ссылку
                            link = ahref['href']
                            index = link.find('?')
                            if index != -1:
                                link = 'https://spimex.com' + link[0:index]
                            else:
                                link = 'https://spimex.com' + link
                            
                            print(f"Обработка файла за {date}: {link}")
                            self.links.append(link)
                            
                            # Скачиваем и обрабатываем Excel файл
                            try:
                                # Читаем Excel файл (пробуем разные движки)
                                try:
                                    df = pd.read_excel(link, engine='xlrd')
                                except:
                                    try:
                                        df = pd.read_excel(link, engine='openpyxl')
                                    except:
                                        df = pd.read_excel(link)
                                
                                # Ищем строку с заголовками
                                header_row = None
                                for idx in range(min(10, len(df))):
                                    row_str = ' '.join([str(cell) for cell in df.iloc[idx].values if pd.notna(cell)])
                                    if any(keyword in row_str.lower() for keyword in ['код', 'наименование', 'инструмент']):
                                        header_row = idx
                                        break
                                
                                # Если нашли заголовки, используем их
                                if header_row is not None:
                                    df = pd.read_excel(link, header=header_row)
                                    # Удаляем первую колонку если она пустая или индексная
                                    if len(df.columns) > 0 and (df.columns[0] == 'Unnamed: 0' or df.columns[0] == 0):
                                        df = df.drop(df.columns[0], axis=1)
                                else:
                                    # Удаляем первую колонку (как в примере)
                                    if len(df.columns) > 0:
                                        df = df.drop(df.columns[0], axis=1)
                                
                                # Удаляем пустые строки
                                df = df.dropna(how='all')
                                
                                # Проверяем количество колонок и переименовываем
                                if len(df.columns) >= 14:
                                    df.columns = ['КодИнструмента', 'НаименованиеИнструмента', 'БазисПоставки', 
                                                'ОбъемДоговоровЕИ', 'ОбъемДоговоровРуб',
                                                'ИзмРынРуб', 'ИзмРынПроц', 'МинЦена', 'СреднЦена', 'МаксЦена', 
                                                'РынЦена', 'ЛучшПредложение',
                                                'ЛучшСпрос', 'КоличествоДоговоров']
                                    
                                    # Удаляем строки, где нет данных
                                    df = df.dropna(subset=['КодИнструмента', 'НаименованиеИнструмента'], how='all')
                                    
                                    # Удаляем строки, где код инструмента пустой или не является строкой/числом
                                    df = df[df['КодИнструмента'].notna()]
                                    df = df[df['КодИнструмента'].astype(str).str.strip() != '']
                                    
                                    # Добавляем дату (преобразуем в строку формата YYYY-MM-DD)
                                    df['Дата'] = date.strftime('%Y-%m-%d')
                                    
                                    # Добавляем поле Товар (извлекаем из НаименованиеИнструмента)
                                    df['Товар'] = df['НаименованиеИнструмента'].apply(
                                        lambda x: str(x).split()[0] if pd.notna(x) and str(x).strip() else None
                                    )
                                    
                                    # Заменяем '-' на NaN
                                    df = df.replace('-', numpy.nan)
                                    
                                    # Сохраняем в базу данных
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
                            # Дата раньше dateFrom - останавливаемся
                            endFlag = True
                            break
                    else:
                        # Дата позже dateTo - продолжаем искать
                        continue
                
                # Если на странице не найдено подходящих ссылок, проверяем следующую
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
