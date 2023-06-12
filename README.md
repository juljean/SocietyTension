Версія Python: 3.9

### Для запуску програми:
- завантажти всі бібліотеки, що задокументовані в requirements.txt;
pip install -r requirements.txt

-запустити функцію main() з файлу interface.py;

-ввести початкову дату інтервалу дослідження напруженості суспільства у форматі yyyy-mm-dd;

-ввести кінцеву дату інтервалу дослідження у форматі yyyy-mm-dd (тестові вибірки для швидшого введення можна взяти в Quick-test.txt).

### Короткий опис кожного компонента:
	
	- yt_api_data_fetch.py - робота з API YouTube для збирання даних про канали, відео та коментарі;
	- data_preprocessing.py - файли для обробки зібраний текстових даних (NLP методи);
	- db_connection.py - усі взаємодії з базою даних PostgreSQL (БД знаходиться на клауд-сервері, тож із наданням паролю, доступні 
        будь-якому користувачу)

	- word2vec_model.py - ініціалізація моделі gensim для векторного огортання слів з текстових даних;
	- k_means_model.py - ініціалізація моделі K-Means для кластеризації векторів і тим самим введення сентименту (залежно від кластеру);
	- means_calculation.py - розрахунок сентименту із знанням кластеру;
	- interface.py - файл для взаємодії з клієнтом і виведення середньодобового сентименту в заданому часовому інтервалі;
    - test_analysis.py - файл для демонстрації роботи програми на ініціалізованих вхідних даних. 

### Детальний опис компонентів:

#### yt_api_data_fetch.py

    Складається з 3-х функцій що виконуються у наступному порядку:
    accessAPI() -> fetch_data_from_yt_api() -> create_dataframe_from_yt_json()
    
    accessAPI() виконує роль посередника, що передає запит з інших файлів до функцій fetch_data_from_yt_api() та
    create_dataframe_from_yt_json();

    fetch_data_from_yt_api() встановлює зв'язок із АПІ Ютубу та запитує статистичні дані для каналу/відео/коментарю;

    create_dataframe_from_yt_json() оброблює отримані дані з АПІ, видаляє непотрібні колонки та створює датафрейм
    структури таблиці із БД.

Для тестування програми:

- у файлі test_analysis запустити test_main_1/test_main_2/test_main_3