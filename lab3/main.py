import tkinter as tk
from tkinter import ttk, messagebox, filedialog, simpledialog
from datetime import datetime, date
import pandas as pd

from program_parser import Parser
from database import Database


class TradesApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Анализ торгов нефтепродуктами")
        self.root.geometry("1200x700")
        
        self.db = Database()
        self.current_data = []

        self.create_widgets()

        self.refresh_data()
    
    def create_widgets(self):

        top_frame = ttk.Frame(self.root, padding="10")
        top_frame.pack(fill=tk.X)
        
        ttk.Button(top_frame, text="Скачать данные с сайта", 
                  command=self.download_data).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(top_frame, text="Обновить данные", 
                  command=self.refresh_data).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(top_frame, text="Экспорт в CSV", 
                  command=self.export_csv).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(top_frame, text="Экспорт в Excel", 
                  command=self.export_excel).pack(side=tk.LEFT, padx=5)
        
        filter_frame = ttk.LabelFrame(self.root, text="Фильтры", padding="10")
        filter_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(filter_frame, text="Дата от:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        self.date_from_entry = ttk.Entry(filter_frame, width=12)
        self.date_from_entry.grid(row=0, column=1, padx=5, pady=5)
        self.date_from_entry.insert(0, "")
        
        ttk.Label(filter_frame, text="Дата до:").grid(row=0, column=2, padx=5, pady=5, sticky=tk.W)
        self.date_to_entry = ttk.Entry(filter_frame, width=12)
        self.date_to_entry.grid(row=0, column=3, padx=5, pady=5)
        self.date_to_entry.insert(0, "")

        ttk.Label(filter_frame, text="Код инструмента:").grid(row=0, column=4, padx=5, pady=5, sticky=tk.W)
        self.instrument_entry = ttk.Entry(filter_frame, width=15)
        self.instrument_entry.grid(row=0, column=5, padx=5, pady=5)

        ttk.Label(filter_frame, text="Цена от:").grid(row=1, column=0, padx=5, pady=5, sticky=tk.W)
        self.price_min_entry = ttk.Entry(filter_frame, width=12)
        self.price_min_entry.grid(row=1, column=1, padx=5, pady=5)
        
        ttk.Label(filter_frame, text="Цена до:").grid(row=1, column=2, padx=5, pady=5, sticky=tk.W)
        self.price_max_entry = ttk.Entry(filter_frame, width=12)
        self.price_max_entry.grid(row=1, column=3, padx=5, pady=5)

        ttk.Label(filter_frame, text="Товар:").grid(row=1, column=4, padx=5, pady=5, sticky=tk.W)
        self.product_entry = ttk.Entry(filter_frame, width=15)
        self.product_entry.grid(row=1, column=5, padx=5, pady=5)
        
        ttk.Button(filter_frame, text="Применить фильтры", 
                  command=self.apply_filters).grid(row=1, column=6, padx=10, pady=5)
        
        ttk.Button(filter_frame, text="Сбросить фильтры", 
                  command=self.reset_filters).grid(row=1, column=7, padx=5, pady=5)
 
        table_frame = ttk.Frame(self.root, padding="10")
        table_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        columns = ('КодИнструмента', 'НаименованиеИнструмента', 'БазисПоставки',
                  'ОбъемДоговоровЕИ', 'ОбъемДоговоровРуб', 'ИзмРынРуб', 'ИзмРынПроц',
                  'МинЦена', 'СреднЦена', 'МаксЦена', 'РынЦена',
                  'ЛучшПредложение', 'ЛучшСпрос', 'КоличествоДоговоров', 'Дата', 'Товар')
        
        self.tree = ttk.Treeview(table_frame, columns=columns, show='headings', height=20)
        column_widths = {
            'КодИнструмента': 120,
            'НаименованиеИнструмента': 200,
            'БазисПоставки': 120,
            'ОбъемДоговоровЕИ': 120,
            'ОбъемДоговоровРуб': 130,
            'ИзмРынРуб': 100,
            'ИзмРынПроц': 100,
            'МинЦена': 100,
            'СреднЦена': 100,
            'МаксЦена': 100,
            'РынЦена': 100,
            'ЛучшПредложение': 130,
            'ЛучшСпрос': 100,
            'КоличествоДоговоров': 150,
            'Дата': 100,
            'Товар': 150
        }
        
        for col in columns:
            self.tree.heading(col, text=col, command=lambda c=col: self.sort_column(c))
            width = column_widths.get(col, 100)
            self.tree.column(col, width=width, anchor=tk.CENTER)
        
        v_scrollbar = ttk.Scrollbar(table_frame, orient=tk.VERTICAL, command=self.tree.yview)
        h_scrollbar = ttk.Scrollbar(table_frame, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
        
        self.tree.grid(row=0, column=0, sticky='nsew')
        v_scrollbar.grid(row=0, column=1, sticky='ns')
        h_scrollbar.grid(row=1, column=0, sticky='ew')
        
        table_frame.grid_rowconfigure(0, weight=1)
        table_frame.grid_columnconfigure(0, weight=1)

        self.status_label = ttk.Label(self.root, text="Готово", relief=tk.SUNKEN)
        self.status_label.pack(fill=tk.X, side=tk.BOTTOM)
        
        self.sort_reverse = {}
    
    def sort_column(self, col):

        reverse = self.sort_reverse.get(col, False)
        self.sort_reverse[col] = not reverse
        
        items = [(self.tree.set(item, col), item) for item in self.tree.get_children('')]

        try:
            items.sort(key=lambda t: float(t[0]) if t[0] else 0, reverse=reverse)
        except:
            items.sort(key=lambda t: str(t[0]), reverse=reverse)

        for index, (val, item) in enumerate(items):
            self.tree.move(item, '', index)
    
    def download_data(self):
        self.status_label.config(text="Скачивание данных с сайта...")
        self.root.update()
        
        try:
            date_from_str = simpledialog.askstring("Дата начала", 
                                                   "Введите дату начала (YYYY-MM-DD):", 
                                                   initialvalue="2025-12-09")
            date_to_str = simpledialog.askstring("Дата конца", 
                                                 "Введите дату конца (YYYY-MM-DD):", 
                                                 initialvalue="2025-12-10")
            
            if not date_from_str or not date_to_str:
                self.status_label.config(text="Отменено пользователем")
                return
            
            try:
                date_from = datetime.strptime(date_from_str, "%Y-%m-%d").date()
                date_to = datetime.strptime(date_to_str, "%Y-%m-%d").date()
            except ValueError:
                messagebox.showerror("Ошибка", "Неверный формат даты. Используйте YYYY-MM-DD")
                self.status_label.config(text="Ошибка формата даты")
                return

            parser = Parser(db_path=self.db.db_path)
            links = parser.getDownloadLinks(date_from, date_to)
            
            if links:
                success_msg = f"Успешно обработано файлов: {len(links)}\nДанные сохранены в базу данных."
                messagebox.showinfo("Успех", success_msg)
                self.status_label.config(text=f"Загружено файлов: {len(links)}")
                self.refresh_data()
            else:
                messagebox.showwarning("Предупреждение", 
                                     "Не найдено файлов для указанного диапазона дат.")
                self.status_label.config(text="Файлы не найдены")
            
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            print(f"Полная ошибка:\n{error_details}")
            messagebox.showerror("Ошибка", 
                               f"Ошибка при получении данных:\n{str(e)}")
            self.status_label.config(text="Ошибка при получении данных")
    
    def apply_filters(self):
        filters = {}
        
        date_from = self.date_from_entry.get().strip()
        if date_from:
            filters['date_from'] = date_from
        
        date_to = self.date_to_entry.get().strip()
        if date_to:
            filters['date_to'] = date_to
        
        instrument = self.instrument_entry.get().strip()
        if instrument:
            filters['instrument_code'] = instrument
        
        price_min = self.price_min_entry.get().strip()
        if price_min:
            try:
                filters['price_min'] = float(price_min)
            except:
                pass
        
        price_max = self.price_max_entry.get().strip()
        if price_max:
            try:
                filters['price_max'] = float(price_max)
            except:
                pass
        
        product = self.product_entry.get().strip()
        if product:
            filters['product'] = product
        
        self.current_data = self.db.get_trades(filters)
        self.update_table()
        self.status_label.config(text=f"Найдено записей: {len(self.current_data)}")
    
    def reset_filters(self):
        self.date_from_entry.delete(0, tk.END)
        self.date_to_entry.delete(0, tk.END)
        self.instrument_entry.delete(0, tk.END)
        self.price_min_entry.delete(0, tk.END)
        self.price_max_entry.delete(0, tk.END)
        self.product_entry.delete(0, tk.END)
        self.refresh_data()
    
    def refresh_data(self):
        self.current_data = self.db.get_trades()
        self.update_table()
        self.status_label.config(text=f"Всего записей: {len(self.current_data)}")
    
    def update_table(self):
        for item in self.tree.get_children():
            self.tree.delete(item)

        for record in self.current_data:
            values = []
            for col in self.tree['columns']:
                value = record.get(col, '')
                if value is None:
                    value = ''
                elif isinstance(value, float):
                    value = f"{value:.2f}" if value else ''
                else:
                    value = str(value)
                values.append(value)
            self.tree.insert('', tk.END, values=values)
    
    def export_csv(self):
        if not self.current_data:
            messagebox.showwarning("Предупреждение", "Нет данных для экспорта")
            return
        
        file_path = filedialog.asksaveasfilename(
            title="Сохранить как CSV",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        
        if not file_path:
            return
        
        try:
            df = pd.DataFrame(self.current_data)
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
            messagebox.showinfo("Успех", f"Данные экспортированы в {file_path}")
            self.status_label.config(text=f"Экспортировано в CSV: {len(self.current_data)} записей")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка при экспорте: {str(e)}")
    
    def export_excel(self):
        if not self.current_data:
            messagebox.showwarning("Предупреждение", "Нет данных для экспорта")
            return
        
        file_path = filedialog.asksaveasfilename(
            title="Сохранить как Excel",
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
        )
        
        if not file_path:
            return
        
        try:
            df = pd.DataFrame(self.current_data)
            df.to_excel(file_path, index=False, engine='openpyxl')
            messagebox.showinfo("Успех", f"Данные экспортированы в {file_path}")
            self.status_label.config(text=f"Экспортировано в Excel: {len(self.current_data)} записей")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка при экспорте: {str(e)}")
    
    def on_closing(self):
        self.db.close()
        self.root.destroy()


def main():
    root = tk.Tk()
    app = TradesApp(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()


if __name__ == "__main__":
    main()
