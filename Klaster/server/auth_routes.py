import logging
from models import db, User
from flask import request, jsonify
from flask_jwt_extended import create_access_token, get_jwt_identity, jwt_required

logger = logging.getLogger(__name__)

def register_auth_routes(app):

    @app.route('/api/register', methods=['POST'])
    def register():
        data = request.get_json()
        if not data:
            return jsonify({"error": "Отсутствует тело запроса JSON"}), 400

        username = data.get('username')
        email = data.get('email')
        password = data.get('password')
        confirm_password = data.get('confirm_password')

        if not username or not email or not password or not confirm_password:
            return jsonify({"error": "Все поля обязательны для регистрации"}), 400

        if password != confirm_password:
            return jsonify({"error": "Пароли не совпадают"}), 400

        if User.query.filter_by(email=email).first():
            return jsonify({"error": "Данный email уже используется"}), 409

        if User.query.filter_by(username=username).first():
            return jsonify({"error": "Данное имя пользователя уже используется"}), 409

        try:
            new_user = User(username=username, email=email)
            new_user.set_password(password)
            db.session.add(new_user)
            db.session.commit()
            logger.info(f"Зарегистрирован пользователь: {username} ({email})")
            return jsonify({"message": "Пользователь успешно зарегистрирован"}), 201
        except Exception as e:
            db.session.rollback()
            logger.error(f"Ошибка при регистрации пользователя {username}: {e}", exc_info=True)
            return jsonify({"error": "Внутренняя ошибка сервера при регистрации"}), 500

    @app.route('/api/login', methods=['POST'])
    def login():
        data = request.get_json()
        if not data:
            return jsonify({"error": "Отсутствует тело запроса JSON"}), 400

        email = data.get('email')
        password = data.get('password')

        if not email or not password:
            return jsonify({"error": "Необходимо указать email и пароль"}), 400

        user = User.query.filter_by(email=email).first()

        if user and user.check_password(password):
            access_token = create_access_token(identity=str(user.id))
            logger.info(f"Пользователь вошел в систему: {user.username} ({email})")
            return jsonify(access_token=access_token, user={'id': user.id, 'username': user.username, 'email': user.email}), 200
        else:
            logger.warning(f"Неудачная попытка входа для email: {email}")
            return jsonify({"error": "Неверный email или пароль"}), 401

    @app.route('/api/me', methods=['GET'])
    @jwt_required()
    def get_current_user():
        current_user_id = get_jwt_identity()
        try:
            user = db.session.get(User, int(current_user_id))
            if user:
                 logger.debug(f"Запрос данных для пользователя ID: {current_user_id}")
                 return jsonify(user={'id': user.id, 'username': user.username, 'email': user.email}), 200
            else:
                 logger.warning(f"Пользователь с ID {current_user_id} не найден в БД (токен валиден).")
                 return jsonify({"error": "Пользователь не найден"}), 404
        except ValueError:
             logger.error(f"Некорректный ID пользователя в токене: {current_user_id}")
             return jsonify({"error": "Некорректный идентификатор пользователя в токене"}), 400
        except Exception as e:
             logger.error(f"Ошибка при получении данных пользователя {current_user_id}: {e}", exc_info=True)
             return jsonify({"error": "Внутренняя ошибка сервера"}), 500