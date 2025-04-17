from flask import request, jsonify
from models import db, User
from flask_jwt_extended import create_access_token, jwt_required, get_jwt_identity
import logging

logger = logging.getLogger(__name__)

def register_auth_routes(app):

    @app.route('/api/register', methods=['POST'])
    def register():
        data = request.get_json()
        username = data.get('username')
        email = data.get('email')
        password = data.get('password')
        confirm_password = data.get('confirm_password')

        if not username or not email or not password or not confirm_password:
            return jsonify({"error": "Все поля обязательны"}), 400

        if password != confirm_password:
            return jsonify({"error": "Пароли не совпадают"}), 400

        if User.query.filter_by(email=email).first():
            return jsonify({"error": "Email уже используется"}), 409

        if User.query.filter_by(username=username).first():
            return jsonify({"error": "Имя пользователя уже используется"}), 409

        try:
            new_user = User(username=username, email=email)
            new_user.set_password(password)
            db.session.add(new_user)
            db.session.commit()
            logger.info(f"User registered: {username} ({email})")
            return jsonify({"message": "Пользователь успешно зарегистрирован"}), 201
        except Exception as e:
            db.session.rollback()
            logger.error(f"Ошибка регистрации пользователя {username}: {e}", exc_info=True)
            return jsonify({"error": "Ошибка регистрации"}), 500

    @app.route('/api/login', methods=['POST'])
    def login():
        data = request.get_json()
        email = data.get('email')
        password = data.get('password')

        if not email or not password:
            return jsonify({"error": "Email и пароль обязательны"}), 400

        user = User.query.filter_by(email=email).first()

        if user and user.check_password(password):
            access_token = create_access_token(identity=str(user.id))

            logger.info(f"User logged in: {user.username} ({email})")
            return jsonify(access_token=access_token, user={'id': user.id, 'username': user.username, 'email': user.email}), 200
        else:
            logger.warning(f"Failed login attempt for email: {email}")
            return jsonify({"error": "Неверный email или пароль"}), 401

    @app.route('/api/me', methods=['GET'])
    @jwt_required()
    def get_current_user():
        current_user_id = get_jwt_identity()
        user = User.query.get(current_user_id)
        if user:
            return jsonify(user={'id': user.id, 'username': user.username, 'email': user.email}), 200
        else:
            return jsonify({"error": "Пользователь не найден"}), 404