import sqlite3
import pandas as pd


class Database:
    def __init__(self, db_path="database.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.create_tables()
    
    def create_tables(self):
        cursor = self.conn.cursor()
    
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS "Table" (
                КодИнструмента TEXT,
                НаименованиеИнструмента TEXT,
                БазисПоставки TEXT,
                ОбъемДоговоровЕИ REAL,
                ОбъемДоговоровРуб REAL,
                ИзмРынРуб REAL,
                ИзмРынПроц REAL
                МинЦена REAL,
                СреднЦена REAL,
                МаксЦена REAL,
                РынЦена REAL,
                ЛучшПредложение REAL,
                ЛучшСпрос REAL,
                КоличествоДоговоров REAL,
                Дата TEXT,
                Товар TEXT
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON "Table"(Дата)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_code ON "Table"(КодИнструмента)')
        
        self.conn.commit()
    
    def get_trades(self, filters=None):
        query = 'SELECT * FROM "Table" WHERE 1=1'
        params = []
        
        if filters:
            if filters.get('date_from'):
                query += ' AND Дата >= ?'
                params.append(filters['date_from'])
            
            if filters.get('date_to'):
                query += ' AND Дата <= ?'
                params.append(filters['date_to'])
            
            if filters.get('instrument_code'):
                query += ' AND КодИнструмента LIKE ?'
                params.append(f"%{filters['instrument_code']}%")
            
            if filters.get('price_min') is not None:
                query += ' AND СреднЦена >= ?'
                params.append(filters['price_min'])
            
            if filters.get('price_max') is not None:
                query += ' AND СреднЦена <= ?'
                params.append(filters['price_max'])
            
            if filters.get('product'):
                query += ' AND (НаименованиеИнструмента LIKE ? OR Товар LIKE ?)'
                params.append(f"%{filters['product']}%")
                params.append(f"%{filters['product']}%")
        
        query += ' ORDER BY Дата DESC, КодИнструмента'
    
        cursor = self.conn.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            rows = cursor.fetchall()

            result = []
            for row in rows:
                if isinstance(row, sqlite3.Row):
                    result.append(dict(row))
                else:
                    columns = [description[0] for description in cursor.description]
                    result.append(dict(zip(columns, row)))
            
            return result
        finally:
            cursor.close()
    
    def close(self):
        if self.conn:
            self.conn.close()
