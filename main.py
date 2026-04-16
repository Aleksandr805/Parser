import sys
import json
import re
import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import random
import time
import os
import urllib.parse
import portalocker  # Для кроссплатформенной блокировки файлов
from typing import Optional, Dict, List, Tuple, Union
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import *

# Глобальный кэш для хранения успешных методов парсинга
site_method_cache = {}

class FileLocker:
    """ Класс для блокировки файлов при многопользовательском доступе """
    @staticmethod
    def lock_file(file_path, timeout=10):
        """ Блокирует файл для записи """
        try:
            lock_file_path = file_path + '.lock'
            try:
                lock_file = open(lock_file_path, 'w')
            except:
                return None
            start_time = time.time()
            while time.time() - start_time < timeout:
                try:
                    portalocker.lock(lock_file, portalocker.LOCK_EX | portalocker.LOCK_NB)
                    return lock_file
                except portalocker.LockException:
                    time.sleep(0.1)
            lock_file.close()
            if os.path.exists(lock_file_path):
                os.remove(lock_file_path)
            return None
        except Exception:
            return None

    @staticmethod
    def unlock_file(lock_file):
        """ Снимает блокировку файла """
        try:
            if lock_file:
                portalocker.unlock(lock_file)
                lock_file.close()
            lock_file_path = lock_file.name
            if os.path.exists(lock_file_path):
                os.remove(lock_file_path)
        except Exception:
            try:
                if lock_file:
                    lock_file.close()
            except:
                pass

class SettingsManager:
    """ Класс для управления настройками приложения """
    def __init__(self):
        self.settings_file = 'settings.json'
        self.default_settings = {
            'selectors_path': 'selectors.json',
            'driver_path': '',
            'max_selectors': 3,
            'request_timeout': 8,
            'selenium_timeout': 10,
            'deviation_formula': '(current_avg * 100 / last_avg) - 100',
            'theme': 'dark'
        }
        self.settings = self.load_settings()

    def load_settings(self):
        try:
            if os.path.exists(self.settings_file):
                with open(self.settings_file, 'r', encoding='utf-8') as f:
                    loaded_settings = json.load(f)
                settings = self.default_settings.copy()
                settings.update(loaded_settings)
                return settings
            else:
                self.save_settings(self.default_settings)
                return self.default_settings.copy()
        except Exception as e:
            print(f"Ошибка загрузки настроек: {str(e)}")
            return self.default_settings.copy()

    def save_settings(self, settings):
        try:
            with open(self.settings_file, 'w', encoding='utf-8') as f:
                json.dump(settings, f, ensure_ascii=False, indent=2)
            self.settings = settings.copy()
            return True
        except Exception as e:
            print(f"Ошибка сохранения настроек: {str(e)}")
            return False

    def reset_to_default(self):
        self.settings = self.default_settings.copy()
        self.save_settings(self.settings)
        return self.settings.copy()

    def get(self, key):
        return self.settings.get(key, self.default_settings.get(key))

    def set(self, key, value):
        self.settings[key] = value

class SelectorsManager:
    """ Класс для управления селекторами с поддержкой многопользовательского доступа """
    def __init__(self, settings_manager):
        self.settings_manager = settings_manager
        self.selectors = {}
        self.lock = None

    def load_selectors(self):
        selectors_path = self.settings_manager.get('selectors_path')
        if not os.path.exists(selectors_path):
            return {}
        try:
            self.lock = FileLocker.lock_file(selectors_path)
            with open(selectors_path, 'r', encoding='utf-8') as f:
                self.selectors = json.load(f)
            self._remove_duplicate_sites()
            return self.selectors.copy()
        except Exception as e:
            print(f"Ошибка загрузки selectors.json: {str(e)}")
            return {}
        finally:
            FileLocker.unlock_file(self.lock)

    def save_selectors(self, selectors):
        selectors_path = self.settings_manager.get('selectors_path')
        try:
            self.lock = FileLocker.lock_file(selectors_path)
            cleaned_selectors = self._remove_duplicate_sites_from_dict(selectors)
            with open(selectors_path, 'w', encoding='utf-8') as f:
                json.dump(cleaned_selectors, f, ensure_ascii=False, indent=2)
            self.selectors = cleaned_selectors.copy()
            return True
        except Exception as e:
            print(f"Ошибка сохранения selectors.json: {str(e)}")
            return False
        finally:
            FileLocker.unlock_file(self.lock)

    def _remove_duplicate_sites(self):
        unique_sites = {}
        seen = set()
        for site, data in self.selectors.items():
            site_name = data.get('site', site)
            if site_name not in seen:
                seen.add(site_name)
                unique_sites[site] = data
        if len(unique_sites) != len(self.selectors):
            self.selectors = unique_sites
            self.save_selectors(self.selectors)

    def _remove_duplicate_sites_from_dict(self, selectors_dict):
        unique_sites = {}
        seen = set()
        for site, data in selectors_dict.items():
            site_name = data.get('site', site)
            if site_name not in seen:
                seen.add(site_name)
                unique_sites[site] = data
        return unique_sites

    def is_site_duplicate(self, site_name, current_site=None):
        site_name_lower = site_name.lower()
        for site, data in self.selectors.items():
            if current_site and site == current_site:
                continue
            existing_site_name = data.get('site', site).lower()
            if existing_site_name == site_name_lower:
                return True
        return False

    def has_duplicate_selectors(self, site_data, current_site=None):
        selectors_set = set()
        max_selectors = self.settings_manager.get('max_selectors')
        for i in range(1, max_selectors + 1):
            selector_key = f'selector{i}'
            type_key = f'selector{i}_type'
            if selector_key in site_data:
                selector = site_data[selector_key].strip()
                selector_type = site_data.get(type_key, 'CSS')
                if selector:
                    selector_tuple = (selector_type, selector)
                    if selector_tuple in selectors_set:
                        return True
                    selectors_set.add(selector_tuple)
        return False

class ParserCore:
    """ Класс с основными функциями парсинга """
    def __init__(self, settings_manager=None):
        self.settings = settings_manager or SettingsManager()
        self.selectors_manager = SelectorsManager(self.settings)

    def get_selector_method(self, selector_data, selector_index):
        method_key = f'selector{selector_index}_method'
        return selector_data.get(method_key, selector_data.get('method', 'Auto'))

    def clean_price(self, text: str) -> Optional[float]:
        if not text: return None
        text_str = str(text).strip()
        price_phrases = ['цена по запросу', 'запрос', 'уточняйте', 'договорная', 'звоните', 'уточнить']
        if any(phrase in text_str.lower() for phrase in price_phrases):
            return None
        text_str = text_str.replace('&nbsp;', ' ').replace('\xa0', ' ').replace('\u2009', ' ').replace('\u202f', ' ').replace('\u00a0', ' ')
        currency_symbols = ['руб.', 'руб', 'р.', 'р', '₽', 'рублей', '€', 'eur', 'euro', 'евро', '$', 'usd', 'долл', 'долларов', 'грн', 'uah', '₴']
        for symbol in currency_symbols:
            text_str = text_str.replace(symbol, '')
        text_str = text_str.replace(',', '.')
        text_str = text_str.replace(' ', '')
        cleaned = re.sub(r'[^\d\.\-]', '', text_str)
        if not cleaned: return None
        if not any(char.isdigit() for char in cleaned): return None
        dot_count = cleaned.count('.')
        if dot_count == 0 or dot_count == 1:
            try: return float(cleaned)
            except ValueError: return None
        else:
            parts = cleaned.split('.')
            if len(parts[-1]) == 2:
                number_str = ''.join(parts[:-1]) + '.' + parts[-1]
                try: return float(number_str)
                except ValueError:
                    try: return float(''.join(parts[:-1]) + parts[-1])
                    except ValueError: return None
            else:
                number_str = cleaned.replace('.', '')
                try: return float(number_str)
                except ValueError: return None

    def check_product_unavailable(self, text: str) -> Optional[str]:
        if not text: return None
        text_lower = str(text).lower().strip()
        unavailable_phrases = ['снято с продажи', 'снят с продажи', 'снята с продажи', 'сняты с продажи', 'нет в наличии', 'распродано', 'закончился', 'завершен']
        for phrase in unavailable_phrases:
            if phrase in text_lower:
                return f"Товар {phrase}"
        return None

    def get_price_requests(self, url: str, selector: str, selector_type: str, timeout: int = None) -> Tuple[Optional[Union[float, str]], bool]:
        try:
            timeout = timeout or self.settings.get('request_timeout')
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            }
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            if selector_type == 'CSS':
                element = soup.select_one(selector)
            else:
                try:
                    from lxml import html
                    tree = html.fromstring(response.content)
                    elements = tree.xpath(selector)
                    element = elements[0] if elements else None
                    if element is not None:
                        price_text = element.text_content()
                        unavailable_msg = self.check_product_unavailable(price_text)
                        if unavailable_msg: return unavailable_msg, True
                        price = self.clean_price(price_text)
                        return price, price is not None
                except ImportError:
                    return None, False
                except Exception:
                    element = None
            if element:
                price_text = element.text
                unavailable_msg = self.check_product_unavailable(price_text)
                if unavailable_msg: return unavailable_msg, True
                price = self.clean_price(price_text)
                return price, price is not None
            else:
                return None, False
        except Exception:
            return None, False

    def get_price_selenium(self, url: str, selector: str, selector_type: str, timeout: int = None) -> Tuple[Optional[Union[float, str]], bool]:
        try:
            timeout = timeout or self.settings.get('selenium_timeout')
            options = webdriver.ChromeOptions()
            options.add_argument('--headless')
            options.add_argument('--disable-gpu')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--log-level=3')
            options.add_experimental_option('excludeSwitches', ['enable-logging'])
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0',
            ]
            options.add_argument(f'user-agent={random.choice(user_agents)}')
            options.add_argument('--disable-web-security')
            options.add_argument('--allow-running-insecure-content')
            options.add_argument('--disable-notifications')
            driver_path = self.settings.get('driver_path')
            driver = None
            try:
                if driver_path and os.path.exists(driver_path):
                    try:
                        from selenium.webdriver.chrome.service import Service
                        service = Service(executable_path=driver_path)
                        driver = webdriver.Chrome(service=service, options=options)
                    except Exception as e:
                        try: driver = webdriver.Chrome(executable_path=driver_path, options=options)
                        except Exception: driver = webdriver.Chrome(options=options)
                else:
                    driver = webdriver.Chrome(options=options)
            except Exception as e:
                return None, False
            if not driver: return None, False
            driver.set_page_load_timeout(timeout)
            try:
                driver.get(url)
                time.sleep(1)
                element = None
                if selector_type == 'CSS':
                    try:
                        element = WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                    except:
                        try:
                            elements = driver.find_elements(By.XPATH, "//*[contains(text(), '₽') or contains(text(), ' руб ') or contains(text(), ' р.')]")
                            if elements: element = elements[0]
                        except: pass
                else:
                    element = WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, selector)))
                if element:
                    price_text = element.text
                    unavailable_msg = self.check_product_unavailable(price_text)
                    if unavailable_msg:
                        driver.quit()
                        return unavailable_msg, True
                    driver.quit()
                    price = self.clean_price(price_text)
                    return price, price is not None
                else:
                    driver.quit()
                    return None, False
            except Exception as e:
                try:
                    page_source = driver.page_source
                    driver.quit()
                    price_patterns = [r'(\d[\d\s]*[.,]\d{2})\s*[₽рРRUBруб]', r'["\']price["\'][\s:]*["\']?([\d\s]+[.,]\d{2})', r'data-price=["\']([\d\s]+[.,]?\d{0,2})']
                    for pattern in price_patterns:
                        match = re.search(pattern, page_source)
                        if match:
                            price_text = match.group(1)
                            price = self.clean_price(price_text)
                            if price: return price, True
                    return None, False
                except:
                    try: driver.quit()
                    except: pass
                    return None, False
        except Exception as e:
            return None, False

class SelectorDialog(QDialog):
    """ Диалог для добавления / редактирования селекторов сайта """
    def __init__(self, site_data=None, parent=None, max_selectors=3, selectors_manager=None):
        super().__init__(parent)
        self.site_data = site_data or {}
        self.max_selectors = max_selectors
        self.selectors_manager = selectors_manager
        self.init_ui()

    def init_ui(self):
        self.setWindowTitle("Добавить / Редактировать сайт" if not self.site_data else "Редактировать сайт")
        self.setMinimumWidth(800)
        self.setMinimumHeight(500)
        layout = QVBoxLayout()
        site_layout = QHBoxLayout()
        site_layout.addWidget(QLabel("Сайт:"))
        self.site_edit = QLineEdit()
        if self.site_data: self.site_edit.setText(self.site_data.get('site', ''))
        site_layout.addWidget(self.site_edit)
        layout.addLayout(site_layout)
        self.selector_edits = []
        for i in range(self.max_selectors):
            group = QGroupBox(f"Селектор {i + 1}")
            group_layout = QVBoxLayout()
            selector_layout = QHBoxLayout()
            selector_layout.addWidget(QLabel("Селектор:"))
            selector_edit = QLineEdit()
            selector_edit.setMinimumWidth(600)
            if self.site_data and f'selector{i + 1}' in self.site_data:
                selector_edit.setText(self.site_data[f'selector{i + 1}'])
            selector_layout.addWidget(selector_edit)
            group_layout.addLayout(selector_layout)
            type_method_layout = QHBoxLayout()
            type_layout = QVBoxLayout()
            type_layout.addWidget(QLabel("Тип селектора:"))
            type_combo = QComboBox()
            type_combo.addItems(["CSS", "XPath"])
            if self.site_data and f'selector{i + 1}_type' in self.site_data:
                idx = type_combo.findText(self.site_data[f'selector{i + 1}_type'])
                if idx >= 0: type_combo.setCurrentIndex(idx)
            type_layout.addWidget(type_combo)
            method_layout = QVBoxLayout()
            method_layout.addWidget(QLabel("Метод парсинга:"))
            method_combo = QComboBox()
            method_combo.addItems(["Auto", "Requests", "Selenium"])
            method_key = f'selector{i + 1}_method'
            method_value = self.site_data.get(method_key, self.site_data.get('method', 'Auto'))
            idx = method_combo.findText(method_value)
            if idx >= 0: method_combo.setCurrentIndex(idx)
            method_layout.addWidget(method_combo)
            type_method_layout.addLayout(type_layout)
            type_method_layout.addSpacing(20)
            type_method_layout.addLayout(method_layout)
            type_method_layout.addStretch()
            group_layout.addLayout(type_method_layout)
            group.setLayout(group_layout)
            layout.addWidget(group)
            self.selector_edits.append((selector_edit, type_combo, method_combo))
        layout.addStretch()
        button_layout = QHBoxLayout()
        self.ok_btn = QPushButton("OK")
        self.ok_btn.clicked.connect(self.validate_and_accept)
        self.cancel_btn = QPushButton("Отмена")
        self.cancel_btn.clicked.connect(self.reject)
        button_layout.addStretch()
        button_layout.addWidget(self.ok_btn)
        button_layout.addWidget(self.cancel_btn)
        layout.addLayout(button_layout)
        self.setLayout(layout)

    def validate_and_accept(self):
        site_name = self.site_edit.text().strip()
        if not site_name:
            QMessageBox.warning(self, "Ошибка", "Название сайта не может быть пустым")
            return
        if self.selectors_manager:
            current_site = self.site_data.get('site') if self.site_data else None
            if self.selectors_manager.is_site_duplicate(site_name, current_site):
                QMessageBox.warning(self, "Ошибка", f"Сайт '{site_name}' уже существует!")
                return
        site_data = self.get_data()
        if self.selectors_manager.has_duplicate_selectors(site_data):
            QMessageBox.warning(self, "Ошибка", "Обнаружены дубликаты селекторов!")
            return
        self.accept()

    def get_data(self):
        data = {'site': self.site_edit.text().strip()}
        for i, (selector_edit, type_combo, method_combo) in enumerate(self.selector_edits):
            selector = selector_edit.text().strip()
            if selector:
                data[f'selector{i + 1}'] = selector
                data[f'selector{i + 1}_type'] = type_combo.currentText()
                data[f'selector{i + 1}_method'] = method_combo.currentText()
        return data

class SheetSelectionDialog(QDialog):
    """ Диалог для выбора вкладок для обработки """
    def __init__(self, sheet_names, parent=None):
        super().__init__(parent)
        self.sheet_names = sheet_names
        self.init_ui()

    def init_ui(self):
        self.setWindowTitle("Выбор вкладок для обработки")
        self.setMinimumWidth(400)
        layout = QVBoxLayout()
        layout.addWidget(QLabel("Выберите вкладки для обработки:"))
        self.checkboxes = []
        for sheet in self.sheet_names:
            cb = QCheckBox(sheet)
            cb.setChecked(True)
            layout.addWidget(cb)
            self.checkboxes.append(cb)
        button_layout = QHBoxLayout()
        self.select_all_btn = QPushButton("Выбрать все")
        self.select_all_btn.clicked.connect(self.select_all)
        self.deselect_all_btn = QPushButton("Снять все")
        self.deselect_all_btn.clicked.connect(self.deselect_all)
        button_layout.addWidget(self.select_all_btn)
        button_layout.addWidget(self.deselect_all_btn)
        button_layout.addStretch()
        layout.addLayout(button_layout)
        button_layout2 = QHBoxLayout()
        self.ok_btn = QPushButton("ОК")
        self.ok_btn.clicked.connect(self.accept)
        self.cancel_btn = QPushButton("Отмена")
        self.cancel_btn.clicked.connect(self.reject)
        button_layout2.addStretch()
        button_layout2.addWidget(self.ok_btn)
        button_layout2.addWidget(self.cancel_btn)
        layout.addLayout(button_layout2)
        self.setLayout(layout)

    def select_all(self):
        for cb in self.checkboxes: cb.setChecked(True)

    def deselect_all(self):
        for cb in self.checkboxes: cb.setChecked(False)

    def get_selected_sheets(self):
        return [sheet for sheet, cb in zip(self.sheet_names, self.checkboxes) if cb.isChecked()]

class TestSelectorThread(QThread):
    """ Поток для тестирования селекторов """
    result = pyqtSignal(str)
    finished = pyqtSignal()
    def __init__(self, url, site, selector_data, parser_core, test_method="Auto", test_specific_selector=None):
        super().__init__()
        self.url = url
        self.site = site
        self.selector_data = selector_data
        self.parser_core = parser_core
        self.test_method = test_method
        self.test_specific_selector = test_specific_selector

    def run(self):
        try:
            self.result.emit(f"Тестируем сайт: {self.site}")
            self.result.emit(f"URL: {self.url}")
            self.result.emit(f"Метод тестирования: {self.test_method}")
            self.result.emit("=" * 50)
            if self.test_specific_selector:
                index, selector, selector_type, selector_method = self.test_specific_selector
                self.result.emit(f"\nТестируем селектор {index} ({selector_type}), метод: {selector_method}:")
                self.result.emit(f"Селектор: {selector}")
                self.test_selector(selector, selector_type, selector_method)
            else:
                for i in range(1, 4):
                    selector_key = f'selector{i}'
                    type_key = f'selector{i}_type'
                    method_key = f'selector{i}_method'
                    if selector_key in self.selector_data:
                        selector = self.selector_data[selector_key]
                        selector_type = self.selector_data.get(type_key, 'CSS')
                        selector_method = self.selector_data.get(method_key, self.selector_data.get('method', 'Auto'))
                        self.result.emit(f"\nСелектор {i} ({selector_type}), метод: {selector_method}:")
                        self.result.emit(f"Селектор: {selector}")
                        self.test_selector(selector, selector_type, selector_method)
        except Exception as e:
            self.result.emit(f"Ошибка при тестировании: {str(e)}")
        finally:
            self.finished.emit()

    def test_selector(self, selector, selector_type, selector_method):
        test_method = selector_method if self.test_method == "Auto" else self.test_method
        if test_method == "Requests" or test_method == "Auto":
            price, success = self.parser_core.get_price_requests(self.url, selector, selector_type)
            if success:
                if isinstance(price, str): self.result.emit(f"Requests: УСПЕХ! Получено сообщение: {price}")
                else: self.result.emit(f"Requests: УСПЕХ! Получена цена: {price}")
            else: self.result.emit(f"Requests: ОШИБКА - не удалось получить цену")
        if test_method == "Selenium" or test_method == "Auto":
            price, success = self.parser_core.get_price_selenium(self.url, selector, selector_type)
            if success:
                if isinstance(price, str): self.result.emit(f"Selenium: УСПЕХ! Получено сообщение: {price}")
                else: self.result.emit(f"Selenium: УСПЕХ! Получена цена: {price}")
            else: self.result.emit(f"Selenium: ОШИБКА - не удалось получить цену")

class SettingsTab(QWidget):
    """ Вкладка настроек приложения """
    def __init__(self, settings_manager, parent=None):
        super().__init__(parent)
        self.settings_manager = settings_manager
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout()
        selectors_layout = QHBoxLayout()
        selectors_layout.addWidget(QLabel("Путь к selectors.json:"))
        self.selectors_path_edit = QLineEdit()
        self.selectors_path_edit.setText(self.settings_manager.get('selectors_path'))
        selectors_layout.addWidget(self.selectors_path_edit)
        self.selectors_browse_btn = QPushButton("Обзор...")
        self.selectors_browse_btn.clicked.connect(self.browse_selectors_file)
        selectors_layout.addWidget(self.selectors_browse_btn)
        layout.addLayout(selectors_layout)
        driver_layout = QHBoxLayout()
        driver_layout.addWidget(QLabel("Путь к ChromeDriver:"))
        self.driver_path_edit = QLineEdit()
        self.driver_path_edit.setText(self.settings_manager.get('driver_path'))
        self.driver_path_edit.setPlaceholderText("Если пусто - используется системный PATH")
        driver_layout.addWidget(self.driver_path_edit)
        self.driver_browse_btn = QPushButton("Обзор...")
        self.driver_browse_btn.clicked.connect(self.browse_driver_file)
        driver_layout.addWidget(self.driver_browse_btn)
        layout.addLayout(driver_layout)
        max_selectors_layout = QHBoxLayout()
        max_selectors_layout.addWidget(QLabel("Макс. селекторов на сайт:"))
        self.max_selectors_spin = QSpinBox()
        self.max_selectors_spin.setRange(1, 10)
        self.max_selectors_spin.setValue(self.settings_manager.get('max_selectors'))
        max_selectors_layout.addWidget(self.max_selectors_spin)
        max_selectors_layout.addStretch()
        layout.addLayout(max_selectors_layout)
        timeouts_group = QGroupBox("Таймауты")
        timeouts_layout = QVBoxLayout()
        request_timeout_layout = QHBoxLayout()
        request_timeout_layout.addWidget(QLabel("Таймаут Requests (сек):"))
        self.request_timeout_spin = QSpinBox()
        self.request_timeout_spin.setRange(1, 60)
        self.request_timeout_spin.setValue(self.settings_manager.get('request_timeout'))
        request_timeout_layout.addWidget(self.request_timeout_spin)
        request_timeout_layout.addStretch()
        timeouts_layout.addLayout(request_timeout_layout)
        selenium_timeout_layout = QHBoxLayout()
        selenium_timeout_layout.addWidget(QLabel("Таймаут Selenium (сек):"))
        self.selenium_timeout_spin = QSpinBox()
        self.selenium_timeout_spin.setRange(1, 60)
        self.selenium_timeout_spin.setValue(self.settings_manager.get('selenium_timeout'))
        selenium_timeout_layout.addWidget(self.selenium_timeout_spin)
        selenium_timeout_layout.addStretch()
        timeouts_layout.addLayout(selenium_timeout_layout)
        timeouts_group.setLayout(timeouts_layout)
        layout.addWidget(timeouts_group)
        formula_group = QGroupBox("Формула расчета Отклонения")
        formula_layout = QVBoxLayout()
        formula_help = QLabel("Используйте переменные: current_avg (новая средняя цена), last_avg (последняя средняя цена)")
        formula_help.setWordWrap(True)
        formula_layout.addWidget(formula_help)
        self.formula_edit = QLineEdit()
        self.formula_edit.setText(self.settings_manager.get('deviation_formula'))
        formula_layout.addWidget(self.formula_edit)
        formula_group.setLayout(formula_layout)
        layout.addWidget(formula_group)
        theme_group = QGroupBox("Тема интерфейса")
        theme_layout = QVBoxLayout()
        self.theme_combo = QComboBox()
        self.theme_combo.addItems(["Темная", "Светлая", "Как в системе"])
        current_theme = self.settings_manager.get('theme')
        if current_theme == 'dark': self.theme_combo.setCurrentIndex(0)
        elif current_theme == 'light': self.theme_combo.setCurrentIndex(1)
        else: self.theme_combo.setCurrentIndex(2)
        theme_layout.addWidget(self.theme_combo)
        theme_note = QLabel("Изменение темы вступит в силу после перезапуска приложения")
        theme_note.setStyleSheet("color: gray; font-style: italic;")
        theme_layout.addWidget(theme_note)
        theme_group.setLayout(theme_layout)
        layout.addWidget(theme_group)
        layout.addStretch()
        button_layout = QHBoxLayout()
        self.save_btn = QPushButton("Сохранить")
        self.save_btn.clicked.connect(self.save_settings)
        self.default_btn = QPushButton("По умолчанию")
        self.default_btn.clicked.connect(self.reset_to_default)
        button_layout.addStretch()
        button_layout.addWidget(self.default_btn)
        button_layout.addWidget(self.save_btn)
        layout.addLayout(button_layout)
        self.setLayout(layout)

    def browse_selectors_file(self):
        file_name, _ = QFileDialog.getOpenFileName(self, "Выберите файл selectors.json", "", "JSON Files (*.json);;All Files (*.*)")
        if file_name: self.selectors_path_edit.setText(file_name)

    def browse_driver_file(self):
        file_name, _ = QFileDialog.getOpenFileName(self, "Выберите файл ChromeDriver", "", "Executable Files (*.exe);;All Files (*.*)")
        if file_name: self.driver_path_edit.setText(file_name)

    def save_settings(self):
        settings = {
            'selectors_path': self.selectors_path_edit.text().strip(),
            'driver_path': self.driver_path_edit.text().strip(),
            'max_selectors': self.max_selectors_spin.value(),
            'request_timeout': self.request_timeout_spin.value(),
            'selenium_timeout': self.selenium_timeout_spin.value(),
            'deviation_formula': self.formula_edit.text().strip(),
            'theme': ['dark', 'light', 'system'][self.theme_combo.currentIndex()]
        }
        if self.settings_manager.save_settings(settings):
            QMessageBox.information(self, "Успех", "Настройки сохранены успешно!\nДля применения некоторых настроек может потребоваться перезапуск приложения.")

    def reset_to_default(self):
        reply = QMessageBox.question(self, 'Подтверждение', 'Вы уверены, что хотите сбросить все настройки к значениям по умолчанию?', QMessageBox.Yes | QMessageBox.No)
        if reply == QMessageBox.Yes:
            default_settings = self.settings_manager.reset_to_default()
            self.selectors_path_edit.setText(default_settings['selectors_path'])
            self.driver_path_edit.setText(default_settings['driver_path'])
            self.max_selectors_spin.setValue(default_settings['max_selectors'])
            self.request_timeout_spin.setValue(default_settings['request_timeout'])
            self.selenium_timeout_spin.setValue(default_settings['selenium_timeout'])
            self.formula_edit.setText(default_settings['deviation_formula'])
            if default_settings['theme'] == 'dark': self.theme_combo.setCurrentIndex(0)
            elif default_settings['theme'] == 'light': self.theme_combo.setCurrentIndex(1)
            else: self.theme_combo.setCurrentIndex(2)
            QMessageBox.information(self, "Успех", "Настройки сброшены к значениям по умолчанию.")

class PriceParserGUI(QMainWindow):
    """Главное окно приложения"""
    def __init__(self):
        super().__init__()
        self.settings_manager = SettingsManager()
        self.parser_core = ParserCore(self.settings_manager)
        self.selectors = {}
        self.current_file = ""
        self.current_site = None
        self.selection_changed = False
        self.parsing_thread = None
        self.is_parsing_stopped = False
        self.init_ui()
        self.load_selectors()

    def init_ui(self):
        self.setWindowTitle("Парсер цен - Универсальный сборщик")
        self.setGeometry(100, 100, 1200, 700)
        self.apply_theme()
        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)
        self.create_main_tab()
        self.create_training_tab()
        self.create_settings_tab()
        self.statusBar().showMessage("Готов к работе")

    def apply_theme(self):
        theme = self.settings_manager.get('theme')
        if theme == 'light':
            self.setStyleSheet("""QMainWindow, QWidget { background-color: #f0f0f0; color: #000000; } QGroupBox { border: 1px solid #cccccc; border-radius: 5px; margin-top: 10px; padding-top: 10px; } QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 5px 0 5px; } QPushButton { background-color: #e0e0e0; border: 1px solid #cccccc; border-radius: 3px; padding: 5px 10px; } QPushButton:hover { background-color: #d0d0d0; } QPushButton:pressed { background-color: #c0c0c0; } QLineEdit, QTextEdit, QComboBox, QSpinBox { background-color: white; border: 1px solid #cccccc; border-radius: 3px; padding: 3px; } QTableWidget { background-color: white; alternate-background-color: #f5f5f5; gridline-color: #dddddd; } QHeaderView::section { background-color: #e0e0e0; padding: 5px; border: 1px solid #cccccc; }""")
        elif theme == 'system':
            self.setStyleSheet("")
        else:
            palette = QPalette()
            palette.setColor(QPalette.Window, QColor(53, 53, 53))
            palette.setColor(QPalette.WindowText, Qt.white)
            palette.setColor(QPalette.Base, QColor(25, 25, 25))
            palette.setColor(QPalette.AlternateBase, QColor(53, 53, 53))
            palette.setColor(QPalette.ToolTipBase, Qt.white)
            palette.setColor(QPalette.ToolTipText, Qt.white)
            palette.setColor(QPalette.Text, Qt.white)
            palette.setColor(QPalette.Button, QColor(53, 53, 53))
            palette.setColor(QPalette.ButtonText, Qt.white)
            palette.setColor(QPalette.BrightText, Qt.red)
            palette.setColor(QPalette.Link, QColor(42, 130, 218))
            palette.setColor(QPalette.Highlight, QColor(42, 130, 218))
            palette.setColor(QPalette.HighlightedText, Qt.black)
            self.setPalette(palette)

    def create_main_tab(self):
        main_widget = QWidget()
        layout = QVBoxLayout()
        file_layout = QHBoxLayout()
        file_layout.addWidget(QLabel("Файл со ссылками:"))
        self.file_edit = QLineEdit()
        self.file_edit.setPlaceholderText("Выберите Excel файл...")
        file_layout.addWidget(self.file_edit)
        self.browse_btn = QPushButton("Обзор...")
        self.browse_btn.clicked.connect(self.browse_file)
        file_layout.addWidget(self.browse_btn)
        self.help_btn = QPushButton("?")
        self.help_btn.setFixedWidth(30)
        self.help_btn.clicked.connect(self.show_help)
        file_layout.addWidget(self.help_btn)
        layout.addLayout(file_layout)
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)
        buttons_layout = QHBoxLayout()
        self.start_btn = QPushButton("Запустить парсинг")
        self.start_btn.clicked.connect(self.start_parsing)
        self.start_btn.setEnabled(False)
        buttons_layout.addWidget(self.start_btn)
        self.stop_btn = QPushButton("Остановить парсинг")
        self.stop_btn.clicked.connect(self.stop_parsing)
        self.stop_btn.setEnabled(False)
        buttons_layout.addWidget(self.stop_btn)
        layout.addLayout(buttons_layout)
        layout.addWidget(QLabel("Логи выполнения:"))
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setFont(QFont("Courier", 9))
        layout.addWidget(self.log_text)
        clear_btn = QPushButton("Очистить логи")
        clear_btn.clicked.connect(self.clear_logs)
        layout.addWidget(clear_btn)
        main_widget.setLayout(layout)
        self.tabs.addTab(main_widget, "Основная работа")

    def create_training_tab(self):
        training_widget = QWidget()
        main_layout = QHBoxLayout()
        left_widget = QWidget()
        left_layout = QVBoxLayout()
        sites_group = QGroupBox("Сайты")
        sites_layout = QVBoxLayout()
        self.sites_list = QListWidget()
        self.sites_list.itemClicked.connect(self.show_site_selectors)
        sites_layout.addWidget(self.sites_list)
        sites_group.setLayout(sites_layout)
        left_layout.addWidget(sites_group)
        buttons_layout = QHBoxLayout()
        self.add_btn = QPushButton("Добавить сайт")
        self.add_btn.clicked.connect(self.add_site)
        buttons_layout.addWidget(self.add_btn)
        self.edit_btn = QPushButton("Редактировать сайт")
        self.edit_btn.clicked.connect(self.edit_site)
        self.edit_btn.setEnabled(False)
        buttons_layout.addWidget(self.edit_btn)
        self.delete_btn = QPushButton("Удалить сайт")
        self.delete_btn.clicked.connect(self.delete_site)
        self.delete_btn.setEnabled(False)
        buttons_layout.addWidget(self.delete_btn)
        left_layout.addLayout(buttons_layout)
        left_widget.setLayout(left_layout)
        main_layout.addWidget(left_widget, 1)
        right_widget = QWidget()
        right_layout = QVBoxLayout()
        self.selectors_group = QGroupBox("Селекторы сайта: не выбран")
        selectors_layout = QVBoxLayout()
        self.selectors_table = QTableWidget(0, 4)
        self.selectors_table.setHorizontalHeaderLabels(["№", "Тип", "Селектор", "Метод"])
        self.selectors_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.selectors_table.setEditTriggers(QTableWidget.DoubleClicked | QTableWidget.EditKeyPressed)
        self.selectors_table.itemChanged.connect(self.on_selector_changed)
        self.selectors_table.cellClicked.connect(self.on_cell_clicked)
        self.selectors_table.setColumnWidth(0, 40)
        self.selectors_table.setColumnWidth(1, 100)
        self.selectors_table.setColumnWidth(2, 500)
        self.selectors_table.setColumnWidth(3, 120)
        self.selectors_table.setMaximumHeight(200)
        selectors_layout.addWidget(self.selectors_table)
        selector_buttons_layout = QHBoxLayout()
        self.add_selector_btn = QPushButton("Добавить селектор")
        self.add_selector_btn.clicked.connect(self.add_selector)
        self.add_selector_btn.setEnabled(False)
        selector_buttons_layout.addWidget(self.add_selector_btn)
        self.delete_selector_btn = QPushButton("Удалить селектор")
        self.delete_selector_btn.clicked.connect(self.delete_selector)
        self.delete_selector_btn.setEnabled(False)
        selector_buttons_layout.addWidget(self.delete_selector_btn)
        self.save_selector_btn = QPushButton("Сохранить селектор")
        self.save_selector_btn.clicked.connect(self.save_selected_selector)
        self.save_selector_btn.setEnabled(False)
        selector_buttons_layout.addWidget(self.save_selector_btn)
        selector_buttons_layout.addStretch()
        selectors_layout.addLayout(selector_buttons_layout)
        self.selectors_group.setLayout(selectors_layout)
        right_layout.addWidget(self.selectors_group, 1)
        test_group = QGroupBox("Тестирование селекторов")
        test_layout = QVBoxLayout()
        test_url_layout = QHBoxLayout()
        test_url_layout.addWidget(QLabel("Тестовый URL:"))
        self.test_url_edit = QLineEdit()
        self.test_url_edit.setPlaceholderText("Введите URL для тестирования селектора")
        test_url_layout.addWidget(self.test_url_edit)
        test_layout.addLayout(test_url_layout)
        test_buttons_layout = QHBoxLayout()
        self.test_selected_btn = QPushButton("Тест выбранного селектора")
        self.test_selected_btn.clicked.connect(self.test_selected_selector)
        self.test_selected_btn.setEnabled(False)
        test_buttons_layout.addWidget(self.test_selected_btn)
        self.test_all_btn = QPushButton("Тест всех селекторов")
        self.test_all_btn.clicked.connect(self.test_all_selectors)
        self.test_all_btn.setEnabled(False)
        test_buttons_layout.addWidget(self.test_all_btn)
        test_buttons_layout.addStretch()
        test_layout.addLayout(test_buttons_layout)
        self.test_result_text = QTextEdit()
        self.test_result_text.setReadOnly(True)
        self.test_result_text.setMaximumHeight(300)
        test_layout.addWidget(self.test_result_text)
        test_group.setLayout(test_layout)
        right_layout.addWidget(test_group, 1)
        right_widget.setLayout(right_layout)
        main_layout.addWidget(right_widget, 3)
        training_widget.setLayout(main_layout)
        self.tabs.addTab(training_widget, "Обучение")

    def create_settings_tab(self):
        self.settings_tab = SettingsTab(self.settings_manager, self)
        self.tabs.addTab(self.settings_tab, "Настройки")

    def browse_file(self):
        file_name, _ = QFileDialog.getOpenFileName(self, "Выберите Excel файл", "", "Excel Files (*.xlsx *.xls)")
        if file_name:
            self.file_edit.setText(file_name)
            self.current_file = file_name
            self.start_btn.setEnabled(True)
            self.log_message(f"Выбран файл: {file_name}")
            try:
                xls = pd.ExcelFile(file_name)
                sheet_names = xls.sheet_names
                self.log_message(f"Найдены вкладки: {', '.join(sheet_names)}")
            except Exception as e:
                self.log_message(f"Ошибка чтения файла: {str(e)}")

    def show_help(self):
        help_text = """Формат файла Excel:

Файл должен содержать колонку 'link' со ссылками на товары (любой сайт).
Парсер автоматически определит домен и подберет нужный селектор.
Собранные цены будут записаны в колонку 'price'.

Пример структуры:
brand | model | link
Товар 1 | М1 | https://tinko.ru/item1
Товар 2 | М2 | https://satro-paladin.com/item2

Остальные колонки (brand, model и т.д.) сохраняются без изменений."""
        QMessageBox.information(self, "Справка по формату файла", help_text)

    def log_message(self, message):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.append(f"[{timestamp}] {message}")
        QApplication.processEvents()

    def clear_logs(self):
        self.log_text.clear()

    def load_selectors(self):
        try:
            self.selectors = self.parser_core.selectors_manager.load_selectors()
            if not self.selectors:
                self.log_message("Селекторы не загружены, создаем дефолтные...")
                self.create_default_selectors()
            else:
                self.update_sites_list()
                self.log_message(f"Загружены селекторы для {len(self.selectors)} сайтов")
        except Exception as e:
            self.log_message(f"Ошибка загрузки selectors.json: {str(e)}")

    def create_default_selectors(self):
        default_selectors = {
            "tinko.ru": {"site": "tinko.ru", "selector1": "#__nuxt > div > main > div > main > div > div.product-detail__content-block > div.product-detail__preview-wrapper > div.product-detail__preview > div.product-detail__content-inner > div.product-detail__row-2 > div.product-detail__prices > div:nth-child(1) > div.product-detail__price > span", "selector1_type": "CSS", "selector1_method": "Auto"},
            "rostov.dean.ru": {"site": "rostov.dean.ru", "selector1": "#appProductPriceVariant > div > div:nth-child(1) > div > div > div:nth-child(2)", "selector1_type": "CSS", "selector1_method": "Auto"},
            "satro-paladin.com": {"site": "satro-paladin.com", "selector1": "body > div.page_content.catalog > div > div > div > div > div > div.details_content > div:nth-child(2) > div.purchase > div.prices > div > span.numbers", "selector1_type": "CSS", "selector1_method": "Auto"},
            "ups-mag.ru": {"site": "ups-mag.ru", "selector1": "body > div.main > div > div > div.main-products > div > div > table > tbody > tr > td.iteminfo.element_right > div.product-price-btn-wrapper > div.product-price-wrapper.price_block > div.price.item_price > span > span", "selector1_type": "CSS", "selector1_method": "Auto"},
            "redutsb.ru": {"site": "redutsb.ru", "selector1": "#product-priceblock > div.product-sales > div.price__block > div", "selector1_type": "CSS", "selector1_method": "Auto"},
            "5000wt.ru": {"site": "5000wt.ru", "selector1": "#price > span", "selector1_type": "CSS", "selector1_method": "Auto"}
        }
        self.selectors = default_selectors
        self.save_selectors()
        self.update_sites_list()

    def save_selectors(self):
        try:
            if self.parser_core.selectors_manager.save_selectors(self.selectors):
                self.log_message("Селекторы успешно сохранены")
            else:
                self.log_message("Ошибка сохранения селекторов")
        except Exception as e:
            self.log_message(f"Ошибка сохранения selectors.json: {str(e)}")

    def update_sites_list(self):
        self.sites_list.clear()
        for site in sorted(self.selectors.keys()):
            self.sites_list.addItem(site)

    def show_site_selectors(self, item):
        site = item.text()
        self.current_site = site
        if site in self.selectors:
            self.selectors_group.setTitle(f"Селекторы сайта: {site}")
            self.selectors_table.blockSignals(True)
            self.selectors_table.setRowCount(0)
            selector_data = self.selectors[site]
            row = 0
            max_selectors = self.settings_manager.get('max_selectors')
            for i in range(1, max_selectors + 1):
                selector_key = f'selector{i}'
                type_key = f'selector{i}_type'
                method_key = f'selector{i}_method'
                if selector_key in selector_data:
                    self.selectors_table.insertRow(row)
                    item_no = QTableWidgetItem(str(i))
                    item_no.setTextAlignment(Qt.AlignCenter)
                    item_no.setFlags(item_no.flags() & ~Qt.ItemIsEditable)
                    self.selectors_table.setItem(row, 0, item_no)
                    selector_type = selector_data.get(type_key, 'CSS')
                    type_combo = QComboBox()
                    type_combo.addItems(["CSS", "XPath"])
                    type_combo.setCurrentText(selector_type)
                    type_combo.currentTextChanged.connect(lambda text, r=row: self.on_cell_changed(r, 1, text))
                    self.selectors_table.setCellWidget(row, 1, type_combo)
                    item_selector = QTableWidgetItem(selector_data[selector_key])
                    self.selectors_table.setItem(row, 2, item_selector)
                    selector_method = selector_data.get(method_key, selector_data.get('method', 'Auto'))
                    method_combo = QComboBox()
                    method_combo.addItems(["Auto", "Requests", "Selenium"])
                    method_combo.setCurrentText(selector_method)
                    method_combo.currentTextChanged.connect(lambda text, r=row: self.on_cell_changed(r, 3, text))
                    self.selectors_table.setCellWidget(row, 3, method_combo)
                    row += 1
            self.edit_btn.setEnabled(True)
            self.delete_btn.setEnabled(True)
            self.test_all_btn.setEnabled(True)
            self.add_selector_btn.setEnabled(True)
            self.delete_selector_btn.setEnabled(row > 0)
            self.selectors_table.clearSelection()
            self.test_selected_btn.setEnabled(False)
            self.save_selector_btn.setEnabled(False)
            self.test_result_text.clear()
            self.selection_changed = False
            self.selectors_table.blockSignals(False)

    def on_cell_changed(self, row, col, value):
        if self.current_site:
            self.selection_changed = True
            self.save_selector_btn.setEnabled(True)

    def on_selector_changed(self, item):
        if item.column() == 2 and self.current_site:
            self.selection_changed = True
            self.save_selector_btn.setEnabled(True)

    def on_cell_clicked(self, row, col):
        if self.current_site:
            self.test_selected_btn.setEnabled(True)
            self.delete_selector_btn.setEnabled(True)

    def add_site(self):
        dialog = SelectorDialog(max_selectors=self.settings_manager.get('max_selectors'), selectors_manager=self.parser_core.selectors_manager)
        if dialog.exec_():
            data = dialog.get_data()
            if data['site']:
                if data['site'] in self.selectors:
                    QMessageBox.warning(self, "Ошибка", f"Сайт '{data['site']}' уже существует!")
                    return
                if self.parser_core.selectors_manager.has_duplicate_selectors(data):
                    QMessageBox.warning(self, "Ошибка", "Обнаружены дубликаты селекторов!")
                    return
                self.selectors[data['site']] = data
                self.save_selectors()
                self.update_sites_list()
                self.log_message(f"Добавлен сайт: {data['site']}")

    def edit_site(self):
        if not self.current_site: return
        if self.current_site in self.selectors:
            dialog = SelectorDialog(self.selectors[self.current_site], max_selectors=self.settings_manager.get('max_selectors'), selectors_manager=self.parser_core.selectors_manager)
            if dialog.exec_():
                data = dialog.get_data()
                if data['site'] and data['site'] != self.current_site:
                    if data['site'] in self.selectors:
                        QMessageBox.warning(self, "Ошибка", f"Сайт '{data['site']}' уже существует!")
                        return
                if self.parser_core.selectors_manager.has_duplicate_selectors(data, self.current_site):
                    QMessageBox.warning(self, "Ошибка", "Обнаружены дубликаты селекторов!")
                    return
                if data['site'] and data['site'] != self.current_site:
                    del self.selectors[self.current_site]
                    self.selectors[data['site']] = data
                    self.current_site = data['site']
                else:
                    self.selectors[self.current_site] = data
                self.save_selectors()
                self.update_sites_list()
                items = self.sites_list.findItems(self.current_site, Qt.MatchExactly)
                if items: self.show_site_selectors(items[0])
                self.log_message(f"Обновлен сайт: {data['site']}")

    def delete_site(self):
        if not self.current_site: return
        if self.current_site in self.selectors:
            reply = QMessageBox.question(self, 'Подтверждение', f'Вы уверены, что хотите удалить сайт "{self.current_site}"?', QMessageBox.Yes | QMessageBox.No)
            if reply == QMessageBox.Yes:
                site_to_delete = self.current_site
                del self.selectors[site_to_delete]
                self.save_selectors()
                self.update_sites_list()
                self.selectors_table.setRowCount(0)
                self.selectors_group.setTitle("Селекторы сайта: не выбран")
                self.test_result_text.clear()
                self.edit_btn.setEnabled(False)
                self.delete_btn.setEnabled(False)
                self.test_selected_btn.setEnabled(False)
                self.test_all_btn.setEnabled(False)
                self.save_selector_btn.setEnabled(False)
                self.add_selector_btn.setEnabled(False)
                self.delete_selector_btn.setEnabled(False)
                self.current_site = None
                self.selection_changed = False
                self.log_message(f"Удален сайт: {site_to_delete}")

    def add_selector(self):
        if not self.current_site: return
        row_count = self.selectors_table.rowCount()
        max_selectors = self.settings_manager.get('max_selectors')
        if row_count >= max_selectors:
            QMessageBox.warning(self, "Ошибка", f"Максимальное количество селекторов - {max_selectors}")
            return
        new_selector_num = row_count + 1
        self.selectors_table.insertRow(row_count)
        item_no = QTableWidgetItem(str(new_selector_num))
        item_no.setTextAlignment(Qt.AlignCenter)
        item_no.setFlags(item_no.flags() & ~Qt.ItemIsEditable)
        self.selectors_table.setItem(row_count, 0, item_no)
        type_combo = QComboBox()
        type_combo.addItems(["CSS", "XPath"])
        type_combo.setCurrentText("CSS")
        type_combo.currentTextChanged.connect(lambda text, r=row_count: self.on_cell_changed(r, 1, text))
        self.selectors_table.setCellWidget(row_count, 1, type_combo)
        item_selector = QTableWidgetItem("")
        self.selectors_table.setItem(row_count, 2, item_selector)
        method_combo = QComboBox()
        method_combo.addItems(["Auto", "Requests", "Selenium"])
        method_combo.setCurrentText("Auto")
        method_combo.currentTextChanged.connect(lambda text, r=row_count: self.on_cell_changed(r, 3, text))
        self.selectors_table.setCellWidget(row_count, 3, method_combo)
        self.delete_selector_btn.setEnabled(True)
        self.selection_changed = True
        self.save_selector_btn.setEnabled(True)

    def delete_selector(self):
        if not self.current_site: return
        selected_row = self.selectors_table.currentRow()
        if selected_row < 0:
            QMessageBox.warning(self, "Ошибка", "Выберите селектор для удаления")
            return
        self.selectors_table.removeRow(selected_row)
        for row in range(self.selectors_table.rowCount()):
            item_no = QTableWidgetItem(str(row + 1))
            item_no.setTextAlignment(Qt.AlignCenter)
            item_no.setFlags(item_no.flags() & ~Qt.ItemIsEditable)
            self.selectors_table.setItem(row, 0, item_no)
        self.delete_selector_btn.setEnabled(self.selectors_table.rowCount() > 0)
        self.selection_changed = True
        self.save_selector_btn.setEnabled(True)

    def save_selected_selector(self):
        if not self.current_site or not self.selection_changed: return
        selector_data = {}
        selector_set = set()
        for row in range(self.selectors_table.rowCount()):
            item_no = self.selectors_table.item(row, 0)
            if not item_no: continue
            selector_num = item_no.text()
            type_widget = self.selectors_table.cellWidget(row, 1)
            if type_widget and isinstance(type_widget, QComboBox): selector_type = type_widget.currentText()
            else: continue
            method_widget = self.selectors_table.cellWidget(row, 3)
            if method_widget and isinstance(method_widget, QComboBox): selector_method = method_widget.currentText()
            else: selector_method = "Auto"
            item_selector = self.selectors_table.item(row, 2)
            if not item_selector or not item_selector.text().strip(): continue
            selector = item_selector.text().strip()
            selector_key = (selector_type, selector, selector_method)
            if selector_key in selector_set:
                QMessageBox.warning(self, "Ошибка", f"Дубликат селектора: {selector} (тип: {selector_type}, метод: {selector_method})")
                return
            selector_set.add(selector_key)
            selector_data[f'selector{selector_num}'] = selector
            selector_data[f'selector{selector_num}_type'] = selector_type
            selector_data[f'selector{selector_num}_method'] = selector_method
        selector_data['method'] = "Auto"
        selector_data['site'] = self.current_site
        self.selectors[self.current_site] = selector_data
        self.save_selectors()
        self.selection_changed = False
        self.save_selector_btn.setEnabled(False)
        self.log_message(f"Сохранены селекторы для сайта: {self.current_site}")
        QMessageBox.information(self, "Сохранение", f"Селекторы для сайта '{self.current_site}' успешно сохранены.")

    def check_url_domain(self, url: str, site: str) -> bool:
        try:
            parsed_url = urllib.parse.urlparse(url)
            if not parsed_url.netloc: return False
            url_domain = parsed_url.netloc.lower()
            if url_domain.startswith('www.'): url_domain = url_domain[4:]
            return site in url_domain
        except Exception:
            return False

    def test_selected_selector(self):
        if not self.current_site or not self.test_url_edit.text().strip():
            QMessageBox.warning(self, "Ошибка", "Выберите сайт и введите тестовый URL")
            return
        selected_row = self.selectors_table.currentRow()
        if selected_row < 0:
            QMessageBox.warning(self, "Ошибка", "Выберите селектор в таблице")
            return
        url = self.test_url_edit.text().strip()
        if not self.check_url_domain(url, self.current_site):
            reply = QMessageBox.warning(self, 'Предупреждение', f'Тестовый URL не соответствует выбранному сайту "{self.current_site}".\nПродолжить тестирование?', QMessageBox.Yes | QMessageBox.No)
            if reply == QMessageBox.No: return
        selector_no = self.selectors_table.item(selected_row, 0).text()
        type_widget = self.selectors_table.cellWidget(selected_row, 1)
        if not type_widget or not isinstance(type_widget, QComboBox):
            QMessageBox.warning(self, "Ошибка", "Не удалось получить тип селектора")
            return
        selector_type = type_widget.currentText()
        method_widget = self.selectors_table.cellWidget(selected_row, 3)
        if not method_widget or not isinstance(method_widget, QComboBox): selector_method = "Auto"
        else: selector_method = method_widget.currentText()
        item_selector = self.selectors_table.item(selected_row, 2)
        if not item_selector:
            QMessageBox.warning(self, "Ошибка", "Не удалось получить селектор")
            return
        selector = item_selector.text()
        self.test_result_text.clear()
        self.test_result_text.append(f"Тестируем селектор {selector_no}...")
        selector_data = self.selectors[self.current_site]
        self.test_thread = TestSelectorThread(url, self.current_site, selector_data, self.parser_core, test_method="Auto", test_specific_selector=(selector_no, selector, selector_type, selector_method))
        self.test_thread.result.connect(self.update_test_results)
        self.test_thread.finished.connect(self.test_finished)
        self.test_thread.start()

    def test_all_selectors(self):
        if not self.current_site or not self.test_url_edit.text().strip():
            QMessageBox.warning(self, "Ошибка", "Выберите сайт и введите тестовый URL")
            return
        url = self.test_url_edit.text().strip()
        if not self.check_url_domain(url, self.current_site):
            reply = QMessageBox.warning(self, 'Предупреждение', f'Тестовый URL не соответствует выбранному сайту "{self.current_site}".\nПродолжить тестирование?', QMessageBox.Yes | QMessageBox.No)
            if reply == QMessageBox.No: return
        self.test_result_text.clear()
        self.test_result_text.append(f"Тестируем все селекторы сайта {self.current_site}...")
        selector_data = self.selectors[self.current_site]
        self.test_thread = TestSelectorThread(url, self.current_site, selector_data, self.parser_core, test_method="Auto")
        self.test_thread.result.connect(self.update_test_results)
        self.test_thread.finished.connect(self.test_finished)
        self.test_thread.start()

    def update_test_results(self, message):
        self.test_result_text.append(message)
        QApplication.processEvents()

    def test_finished(self):
        self.test_result_text.append("\nТестирование завершено!")

    def start_parsing(self):
        if not self.current_file or not os.path.exists(self.current_file):
            QMessageBox.warning(self, "Ошибка", "Файл не выбран или не существует")
            return
        try:
            xls = pd.ExcelFile(self.current_file)
            sheet_names = xls.sheet_names
            if not sheet_names:
                QMessageBox.warning(self, "Ошибка", "В файле нет вкладки")
                return
            if len(sheet_names) > 1:
                dialog = SheetSelectionDialog(sheet_names, self)
                if dialog.exec_():
                    selected_sheets = dialog.get_selected_sheets()
                    if not selected_sheets:
                        QMessageBox.warning(self, "Ошибка", "Не выбрано ни одной вкладки для обработки")
                        return
                else: return
            else:
                selected_sheets = sheet_names
            self.log_message(f"Выбраны для обработки: {', '.join(selected_sheets)}")
            self.start_btn.setEnabled(False)
            self.stop_btn.setEnabled(True)
            self.progress_bar.setVisible(True)
            self.progress_bar.setValue(0)
            self.is_parsing_stopped = False
            self.parsing_thread = ParsingThread(self.current_file, self.selectors, selected_sheets, self.parser_core)
            self.parsing_thread.progress.connect(self.update_progress)
            self.parsing_thread.message.connect(self.log_message)
            self.parsing_thread.finished.connect(self.parsing_finished)
            self.parsing_thread.start()
        except Exception as e:
            self.log_message(f"Ошибка подготовки к парсингу: {str(e)}")
            self.start_btn.setEnabled(True)
            self.stop_btn.setEnabled(False)
            self.progress_bar.setVisible(False)

    def stop_parsing(self):
        if self.parsing_thread and self.parsing_thread.isRunning():
            self.is_parsing_stopped = True
            self.parsing_thread.stop()
            self.log_message("Останавливаю парсинг...")
            self.stop_btn.setEnabled(False)

    def update_progress(self, value):
        self.progress_bar.setValue(value)

    def parsing_finished(self, success):
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.progress_bar.setVisible(False)
        if success and not self.is_parsing_stopped:
            self.log_message("Парсинг успешно завершен!")
            QMessageBox.information(self, "Успех", "Парсинг цен завершен успешно!")
        elif self.is_parsing_stopped:
            self.log_message("Парсинг остановлен пользователем")
            QMessageBox.information(self, "Остановка", "Парсинг остановлен. Данные сохранены.")
        else:
            self.log_message("Парсинг завершен с ошибками")
            QMessageBox.warning(self, "Предупреждение", "Парсинг завершен с ошибками. Проверьте логи.")

class ParsingThread(QThread):
    """ Поток для выполнения парсинга с возможностью остановки """
    progress = pyqtSignal(int)
    message = pyqtSignal(str)
    finished = pyqtSignal(bool)

    def __init__(self, file_path, selectors, sheets_to_process, parser_core):
        super().__init__()
        self.file_path = file_path
        self.selectors = selectors
        self.sheets_to_process = sheets_to_process
        self.parser_core = parser_core
        self.site_method_cache = {}
        self.total_tasks = 0
        self.completed_tasks = 0
        self.is_running = True

    def stop(self):
        self.is_running = False

    def get_site_from_url(self, url):
        """ Автоматически определяет сайт по домену из URL """
        try:
            parsed = urllib.parse.urlparse(url)
            if not parsed.netloc: return None
            url_domain = parsed.netloc.lower()
            if url_domain.startswith('www.'): url_domain = url_domain[4:]
            # Ищем совпадение среди известных сайтов
            for site in self.selectors.keys():
                if site in url_domain:
                    return site
        except Exception:
            pass
        return None

    def run(self):
        try:
            self.message.emit(f"Начинаем парсинг файла: {self.file_path}")
            all_sheet_data = {}
            try:
                xls = pd.ExcelFile(self.file_path)
                for sheet_name in self.sheets_to_process:
                    if sheet_name in xls.sheet_names:
                        all_sheet_data[sheet_name] = pd.read_excel(xls, sheet_name=sheet_name)
                xls.close()
            except Exception as e:
                self.message.emit(f"Ошибка чтения файла: {str(e)}")
                self.finished.emit(False)
                return

            self.calculate_total_tasks_from_data(all_sheet_data)
            total_sheets = len(self.sheets_to_process)
            processed_data = {}

            for sheet_idx, sheet_name in enumerate(self.sheets_to_process):
                if not self.is_running: break
                self.message.emit(f"Обработка листа ({sheet_idx + 1}/{total_sheets}): {sheet_name}")
                if sheet_name not in all_sheet_data:
                    self.message.emit(f"Лист {sheet_name} не найден в файле")
                    continue
                df = all_sheet_data[sheet_name].copy()
                total_rows = len(df)
                self.message.emit(f"Найдено {total_rows} строк для обработки")
                df = self.parse_prices_for_sheet(df, total_rows, sheet_name)
                if not self.is_running:
                    self.message.emit("Парсинг остановлен, сохраняем текущие данные...")
                processed_data[sheet_name] = df
                if not self.is_running: break

            try:
                import tempfile
                import shutil
                fd, temp_path = tempfile.mkstemp(suffix='.xlsx', prefix='tmp_')
                os.close(fd)
                with pd.ExcelWriter(temp_path, engine='openpyxl') as writer:
                    for sheet_name, df in processed_data.items():
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                try: import gc; gc.collect()
                except: pass
                shutil.copy2(temp_path, self.file_path)
                os.remove(temp_path)
                self.message.emit(f"Результаты успешно сохранены в исходный файл: {self.file_path}")
            except Exception as e:
                self.message.emit(f"Ошибка сохранения файла: {str(e)}")
                backup_path = os.path.splitext(self.file_path)[0] + '_backup.xlsx'
                try:
                    with pd.ExcelWriter(backup_path, engine='openpyxl') as writer:
                        for sheet_name, df in processed_data.items():
                            df.to_excel(writer, sheet_name=sheet_name, index=False)
                    self.message.emit(f"Данные сохранены в резервный файл: {backup_path}")
                except Exception as e2:
                    self.message.emit(f"Не удалось сохранить даже в резервный файл: {e2}")
                self.finished.emit(False)
                return

            self.progress.emit(100)
            self.finished.emit(True if self.is_running else False)
        except Exception as e:
            self.message.emit(f"Критическая ошибка: {str(e)}")
            self.finished.emit(False)

    def calculate_total_tasks_from_data(self, all_sheet_data):
        """Подсчитывает общее количество задач на основе данных в памяти (единая колонка link)"""
        self.total_tasks = 0
        for sheet_name, df in all_sheet_data.items():
            if 'link' in df.columns:
                if 'price' in df.columns:
                    count = df[(df['link'].notna()) & ((df['price'].isna()) | (df['price'] == ''))].shape[0]
                else:
                    count = df['link'].notna().sum()
                self.total_tasks += count
        if self.total_tasks == 0:
            self.total_tasks = 1

    def parse_prices_for_sheet(self, df, total_rows, sheet_name):
        """ Парсит цены из единой колонки 'link' и записывает в 'price' """
        if 'link' not in df.columns:
            self.message.emit(f"⚠️ Колонка 'link' не найдена в листе {sheet_name}")
            return df

        if 'price' not in df.columns:
            df['price'] = None

        mask = df['link'].notna() & ((df['price'].isna()) | (df['price'] == ''))
        non_empty_urls = mask.sum()
        self.message.emit(f"🔍 Парсим лист {sheet_name} (найдено {non_empty_urls} url из {total_rows} строк)...")

        processed_count = 0
        for idx in df[mask].index:
            if not self.is_running: break

            url = str(df.at[idx, 'link']).strip()
            processed_count += 1
            if processed_count % 5 == 0:
                self.message.emit(f" Обработано {processed_count}/{non_empty_urls} строк")

            self.completed_tasks += 1
            if self.total_tasks > 0:
                progress = int((self.completed_tasks / self.total_tasks) * 100)
                self.progress.emit(progress)

            site = self.get_site_from_url(url)
            if site and site in self.selectors:
                selector_data = self.selectors[site]
                price = self.parse_price_with_method(url, site, selector_data)
                df.at[idx, 'price'] = price
            else:
                df.at[idx, 'price'] = "Не известный сайт"

        if processed_count > 0:
            self.message.emit(f" Обработано {processed_count}/{non_empty_urls} строк")
        return df

    def parse_price_with_method(self, url, site, selector_data):
        """ Парсит цену с использованием указанного метода """
        if not url or pd.isna(url) or str(url).strip() == '': return "Пустая ссылка"
        if site not in self.selectors: return "Не известный сайт"

        has_selectors = False
        max_selectors = self.parser_core.settings.get('max_selectors')
        for i in range(1, max_selectors + 1):
            if f'selector{i}' in selector_data:
                has_selectors = True
                break
        if not has_selectors: return "Нет селектора"

        price = None
        success = False
        for i in range(1, max_selectors + 1):
            if not self.is_running: break
            selector_key = f'selector{i}'
            type_key = f'selector{i}_type'
            method_key = f'selector{i}_method'

            if selector_key in selector_data:
                selector_method = selector_data.get(method_key, selector_data.get('method', 'Auto'))
                if selector_method == "Requests":
                    price, success = self.parser_core.get_price_requests(url, selector_data[selector_key], selector_data.get(type_key, 'CSS'))
                    if success:
                        if isinstance(price, str): return price
                        elif price is not None:
                            self.site_method_cache[f"{site}_selector{i}"] = "Requests"
                            return price
                elif selector_method == "Selenium":
                    price, success = self.parser_core.get_price_selenium(url, selector_data[selector_key], selector_data.get(type_key, 'CSS'))
                    if success:
                        if isinstance(price, str): return price
                        elif price is not None:
                            self.site_method_cache[f"{site}_selector{i}"] = "Selenium"
                            return price
                else:  # Auto
                    price, success = self.parser_core.get_price_requests(url, selector_data[selector_key], selector_data.get(type_key, 'CSS'))
                    if success:
                        if isinstance(price, str): return price
                        elif price is not None:
                            self.site_method_cache[f"{site}_selector{i}"] = "Requests"
                            return price
                    price, success = self.parser_core.get_price_selenium(url, selector_data[selector_key], selector_data.get(type_key, 'CSS'))
                    if success:
                        if isinstance(price, str): return price
                        elif price is not None:
                            self.site_method_cache[f"{site}_selector{i}"] = "Selenium"
                            return price
        return "Не корректный селектор"

def main():
    """Точка входа в приложение"""
    try:
        app = QApplication(sys.argv)
        app.setStyle('Fusion')
        window = PriceParserGUI()
        window.show()
        sys.exit(app.exec_())
    except Exception as e:
        print(f"Ошибка запуска приложения: {str(e)}")
        print("Убедитесь, что установлены все зависимости:")
        print("pip install pandas openpyxl requests beautifulsoup4 selenium pyqt5 lxml portalocker")
        input("Нажмите Enter для выхода...")

if __name__ == "__main__":
    main()
