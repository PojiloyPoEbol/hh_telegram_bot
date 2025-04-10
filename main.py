import requests
import pandas as pd
import json
import openpyxl
from api import AbstractDbAPI
import telebot
from telebot import types
from test_matcher import VacancyMatcher
from psycopg2 import sql
import psycopg2

def get_area(name):
    req = requests.get('https://api.hh.ru/areas')
    data = req.content.decode()
    req.close()
    jsObj = json.loads(data)
    areas = []
    for k in jsObj:
        for i in range(len(k['areas'])):
            if len(k['areas'][i]['areas']) != 0:
                for j in range(len(k['areas'][i]['areas'])):
                    areas.append([k['id'],
                                  k['name'],
                                  k['areas'][i]['areas'][j]['id'],
                                  k['areas'][i]['areas'][j]['name']])
            else:
                areas.append([k['id'],
                              k['name'],
                              k['areas'][i]['id'],
                              k['areas'][i]['name']])

    text = ''
    for obj in areas:
        text += str(obj)
    for obj in areas:
        text = text.replace("'", "")
        text = text.replace("[", "")
        text = text.replace("]", ",")
        text = text.replace(" ", "")
    text = text.split(",")
    for objj in text:
        if objj == name.replace(" ",""):
            ind = int(text.index(name.replace(" ","")))
            area_id = text[ind - 1]
            obj_ar = f"{objj} {area_id}"
            return obj_ar
            break
        else:
            continue



def script(name,area):
  url = "https://api.hh.ru/vacancies"
  area1 = get_area(area)
  print(area1)
  area1 = area1.split(" ") #0 - Регион 1 - id
  params = {
    'text': f'NAME:{name}',
    'area': int(area1[1]),
    'page': 0,
    'per_page': 100
  }
  r = requests.get(url=url, params=params)
  r_json = r.json()
  df = pd.DataFrame(columns=['external_id','title','area_name','area_id' , 'company_name','required_skills', 'vacancy_link'])
  for i in r_json['items']:
    idi = i['id']#айди блин вакансии там PS. это первый и последний коммент от меня :)
    r1 = requests.get(url= f'https://api.hh.ru/vacancies/{idi}')
    r1_json = r1.json()
    title = r1_json['name']

    try:
        skills = r1_json['key_skills']
        sk = ''
        for j in skills:
            j = str(j).split("'")
            j = j[3]
            sk = sk + j + ','
        sk = sk[:-1]
        if skills == []:
            sk = ''
    except:
        sk = ''
    emp = r1_json['employer']['name']
    alt = r1_json['alternate_url']
    df.loc[len(df)] = [idi, title,area1[0],area1[1], emp, sk, alt]
  print(df)
  df.to_excel('hh.xlsx')
  return df

api = AbstractDbAPI
engine = api.create_conn('databaseinf.txt','localhost','5432','postgres')

user_data = {}

# Создаем DataFrame для хранения всех данных
df = pd.DataFrame(columns=['name', 'group_name', 'buisnes_role', 'skills'])

# Список бизнес-ролей (для кнопок)
business_roles = [
    "Data-engineer",
    ".NET",
    "Разработчик игр",
    "Реклама",
    "Веб разработчик"
]

token = 'your_token'

bot = telebot.TeleBot(token)

@bot.message_handler(commands=['start'])
def start_echo(message):
    bot.send_message(message.chat.id,"Привет этот бот предназначен для мониторинга вакансий с HeadHunter в разрезе бизнес-ролей колледжа"
                                     "\nСписок комманд:\n"
                                     "/start\n"
                                     "/button\n"
                                     )



@bot.message_handler(content_types=["document"])
def handle_doc(message):
  document_id = message.document.file_id
  file_info = bot.get_file(document_id)
  print(document_id) # Выводим file_id
  print(f'http://api.telegram.org/file/bot{token}/{file_info.file_path}')
  bot.send_message(message.chat.id, "Бот предназначен только для оброаботки текстовых запросов")

@bot.message_handler(content_types=["photo"])
def handle_photo(message):
  photo = max(message.photo, key=lambda x: x.height)
  photo_id = photo.file_id
  file_info = bot.get_file(photo_id)
  print(photo_id) # Выводим file_id
  print(f'http://api.telegram.org/file/bot{token}/{file_info.file_path}')
  bot.send_message(message.chat.id, "Бот предназначен только для оброаботки текстовых запросов")

@bot.message_handler(content_types=["audio"])
def handle_audio(message):
  audio_id = message.audio.file_id
  file_info = bot.get_file(audio_id)
  print(audio_id)
  print(f'http://api.telegram.org/file/bot{token}/{file_info.file_path}')
  bot.send_message(message.chat.id, "Бот предназначен только для оброаботки текстовых запросов")

@bot.message_handler(content_types=["video"])
def handle_video(message):
 video_id=message.video.file_id
 file_info = bot.get_file(video_id)
 print(video_id)
 print(f'http://api.telegram.org/file/bot{token}/{file_info.file_path}')
 bot.send_message(message.chat.id, "Бот предназначен только для оброаботки текстовых запросов")

@bot.message_handler(content_types=["voice"])
def handle_voice(message):
 voice_id=message.voice.file_id
 file_info = bot.get_file(voice_id)
 print(voice_id)
 print(f'http://api.telegram.org/file/bot{token}/{file_info.file_path}')
 bot.send_message(message.chat.id, "Бот предназначен только для оброаботки текстовых запросов")

@bot.message_handler(commands=['button'])
def button_ms1(message):
  markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
  item1 = types.KeyboardButton("Поиск по бизнес роли")
  item2 = types.KeyboardButton("Поиск по ключевым навыкам")
  markup.add(item1, item2)
  bot.send_message(message.chat.id, "Выберете способ поиска", reply_markup=markup)

@bot.message_handler(content_types='text')
def button_reply(message):
    print(message.text)
    try:
        mssg = message.text
        if mssg == 'Поиск по бизнес роли':
            bot.send_message(message.chat.id, "Укажите бизнес роль и область поиска(город) через запятую!")
            bot.register_next_step_handler(message, message_echo)
        elif mssg =='tr_vac':
            api.execute(engine,"""
            TRUNCATE TABLE si.vacancies CASCADE
            """)
            bot.send_message(message.chat.id,
                             "Да мой господин!")

        elif mssg == 'Поиск по ключевым навыкам':
            user_id = message.chat.id
            user_data[user_id] = {"name":None,
                                  "group_name": None,
                                  "buisnes_role":None,
                                  "skills":None}  # Создаем запись для пользователя

            bot.send_message(
                user_id,
                "Привет! Давай соберем твои данные.\n\n"
                "Укажи свое имя и фамилию:"
            )
        else:
            user_id = message.chat.id

            if user_data[user_id]["name"] == None:
                get_name(message)
            elif user_data[user_id]["group_name"] == None:
                get_group(message)
            elif user_data[user_id]["buisnes_role"] == None:
                get_business_role(message)
            elif user_data[user_id]["skills"] == None:
                get_skills(message)
    except Exception as e:
        print(f"Ошибка: {repr(e)}")
        bot.send_message(message.chat.id,"Что-то пошло не так попробуйте еще раз")
# Обработчик текстовых сообщений (имя и фамилия)
@bot.message_handler(func=lambda m: m.chat.id in user_data and "Имя и фамилия" not in user_data[m.chat.id])
def get_name(message):
    user_id = message.chat.id
    user_data[user_id]["name"] = message.text

    bot.send_message(user_id, "Отлично! Теперь укажи свою группу:")

# Обработчик текстовых сообщений (группа)
@bot.message_handler(func=lambda m: m.chat.id in user_data and "Группа" not in user_data[m.chat.id] and "Имя и фамилия" in user_data[m.chat.id])
def get_group(message):
    user_id = message.chat.id
    user_data[user_id]["group_name"] = message.text

    # Создаем клавиатуру с бизнес-ролями
    markup = types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
    for role in business_roles:
        markup.add(types.KeyboardButton(role))

    bot.send_message(
        user_id,
        "Хорошо! Теперь выбери свою бизнес-роль:",
        reply_markup=markup
    )

# Обработчик выбора бизнес-роли (кнопки)
@bot.message_handler(func=lambda m: m.chat.id in user_data and "Бизнес-роль" not in user_data[m.chat.id] and m.text in business_roles)
def get_business_role(message):
    user_id = message.chat.id
    user_data[user_id]["buisnes_role"] = message.text

    # Убираем клавиатуру
    markup = types.ReplyKeyboardRemove()

    bot.send_message(
        user_id,
        "Отлично! Теперь укажи свои ключевые навыки (через запятую):",
        reply_markup=markup
    )

# Обработчик ключевых навыков
@bot.message_handler(func=lambda m: m.chat.id in user_data and "Ключевые навыки" not in user_data[m.chat.id] and "Бизнес-роль" in user_data[m.chat.id])
def get_skills(message):
    user_id = message.chat.id
    user_data[user_id]["skills"] = message.text

    # Добавляем данные в DataFrame
    global df
    df = pd.concat([df, pd.DataFrame([user_data[user_id]])], ignore_index=True)
    print(df)
    # Сохраняем DataFrame в БД
    df.to_sql('students',schema='si',con=engine, index=False, if_exists='append')

    api.execute(engine, """
    WITH CTE_duplicates AS (
      SELECT student_id, ROW_NUMBER() OVER (PARTITION BY name ORDER BY student_id) AS row_number
      FROM si.students
    )
    DELETE FROM si.students
    WHERE student_id IN (SELECT student_id FROM CTE_duplicates WHERE row_number > 1);
    """)
    bot.send_message(
        user_id,
        "Спасибо! Твои данные сохранены.\n\n"
        f"Имя: {user_data[user_id]['name']}\n"
        f"Группа: {user_data[user_id]['group_name']}\n"
        f"Роль: {user_data[user_id]['buisnes_role']}\n"
        f"Навыки: {user_data[user_id]['skills']}"
    )
    bot.send_message(
        user_id,
        "Отлично! Теперь укажи город для поиска вакансий:",
    )
    bot.register_next_step_handler(message, message_search)


def message_search(message):
    user_id = message.chat.id
    mssg = message.text
    bot.send_message(
        user_id,
        "Выполняется поиск пожалуйста подождите...",
    )
    df = script('Data engineer',mssg)
    df.to_sql('vacancies', schema='si', con=engine, index=False, if_exists='append')

    df = script('.NET', mssg)
    df.to_sql('vacancies', schema='si', con=engine, index=False, if_exists='append')

    df = script('Разработчик игр', mssg)
    df.to_sql('vacancies', schema='si', con=engine, index=False, if_exists='append')

    df = script('Реклама', mssg)
    df.to_sql('vacancies', schema='si', con=engine, index=False, if_exists='append')

    df = script('Веб разработчик', mssg)
    df.to_sql('vacancies', schema='si', con=engine, index=False, if_exists='append')

    bot.send_message(
        user_id,
        "Загрузка завершена!",
    )
    api.execute(engine, """
    WITH CTE_duplicates AS (
      SELECT vacancy_id, ROW_NUMBER() OVER (PARTITION BY external_id ORDER BY vacancy_id) AS row_number
      FROM si.vacancies
    )
    DELETE FROM si.vacancies
    WHERE vacancy_id IN (SELECT vacancy_id FROM CTE_duplicates WHERE row_number > 1);
    """)
# insert your parameters
    db_params = {
        'host': '',
        'database': '',
        'user': '',
        'password': ''
    }
# insert your parameters here too
    conn = psycopg2.connect(
        dbname="",
        user="",
        password="",
        host=""
    )


    matcher = VacancyMatcher(db_params)
    recommendations = matcher.recommend_vacancies(user_data[user_id]['name'])

    bot.send_message(user_id,"Рекомендованные вакансии:")
    for vac in recommendations:
        bot.send_message(user_id, f"\n{vac['title']} в {vac['company']} ({vac['location']})\n"
                                  f"Требуемые навыки: {vac['required_skills']}\n"
                                  f"Совпадение навыков: {vac['match_score']}")
        bot.send_message(user_id, f"Ссылка: {vac['link']}\n\n")


    cur = conn.cursor()
    cur.execute(f"select student_id from si.students where name = '{user_data[user_id]['name']}'")
    for stud in cur.fetchall():
        s_id = stud[0]
    matches = pd.DataFrame(columns=['student_id','vacancy_id','match_score',])
    for vac in recommendations:
        cur.execute(f"select vacancy_id from si.vacancies where vacancy_link = '{vac['link']}'")
        for vac_id in cur.fetchall():
            v_id = vac_id[0]
        matches.loc[len(matches)] = [s_id,v_id,float(str(vac['match_score']).replace('%',''))]
    matches.to_sql('matches', schema='si', con=engine, index=False, if_exists='append')

    # Очищаем временные данные
    del user_data[user_id]


def message_echo(message):
    try:
        mssg = message.text
        mssg1 = mssg.split(",")
        bot.send_message(message.chat.id, f"Поиск вакансии {mssg1[0]} в области: {mssg1[1]}")
        name = mssg1[0]
        region = mssg1[1]
        script(name, region)
        bot.send_document(message.chat.id, open(r'D:\PycharmProject\pythonProject10\hh.xlsx', 'rb'))
    except:
        bot.send_message(message.chat.id, "Прошу не ломайте меня и попробуйте снова(")

bot.polling(none_stop=True)
