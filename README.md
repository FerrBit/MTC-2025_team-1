# Класстеризация

## Технологический стек

*   **Фронтенд:** React, TypeScript, CSS
*   **Бэкенд:** Flask (Python)
*   **База данных:** SQLAlchemy ORM (поддерживает SQLite по умолчанию, PostgreSQL и др.)
*   **Миграции БД:** Flask-Migrate
*   **Аутентификация:** Flask-JWT-Extended (JWT токены), Flask-Bcrypt (хэширование паролей)

## Предварительные требования
```bash
python3 -m venv .venv
source .venv/bin/activate
```
Windows (Git Bash/PowerShell)

```bash
python -m venv .venv
.\.venv\Scripts\activate
```

Установите зависимости Python

```bash
pip install -r requirements.txt
```

Создайте файл .env в папке server/
и добавьте в него следующие переменные:

```bash
touch .env
```

**Содержимое файла .env:**

Обязательно: Секретный ключ для подписи JWT токенов
Пример генерации в Python: 

```bash
python -c 'import secrets; print(secrets.token_hex(32))'
```

`JWT_SECRET_KEY=ваш_очень_секретный_ключ_для_jwt`

Обязательно: Секретный ключ Flask (для сессий, защиты от CSRF и т.д.)
Пример генерации в Python: 

```bash
python -c 'import secrets; print(secrets.token_hex(32))'
```

`FLASK_SECRET_KEY=ваш_другой_очень_секретный_ключ_для_flask`

**3. Настройка базы данных (Flask-Migrate):**

Если вы запускаете проект ВПЕРВЫЕ и папки migrations еще нет:

```bash
flask db init
flask db migrate -m "Initial migration."
flask db upgrade
```

Если в будущем вы измените модели (models.py), повторите:

```bash
flask db migrate -m "Краткое описание изменений"
flask db upgrade
```

**4. Настройка Фронтенда (Клиент):**

Перейдите в папку клиента (из корневой папки проекта)

```bash
cd ../client
```

Установите зависимости Node.js

```bash
npm install
```

## Запуск Приложения

**1. Запустите Бэкенд (Сервер):**

Откройте новый терминал

```bash
cd server
flask run
```

Сервер будет доступен по адресу http://127.0.0.1:5000 (или http://localhost:5000)

**2. Запустите Фронтенд (Клиент):**

Откройте новый терминал

```bash
cd client
npm start
```

Фронтенд будет доступен по адресу http://localhost:3000