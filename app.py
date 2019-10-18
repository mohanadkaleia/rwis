import user as user_service

from flask import request, make_response, jsonify, Flask

app = Flask(__name__)


@app.route('/')
def hello():
    return "Hello World!"


@app.route('/login', methods=['POST'])
def login_view():
    name = request.form.get('name', None)
    password = request.form.get('password', None)
    print('!!!2')
    if not name or not password:
        return make_response(jsonify({'status': 'fail', 'message': 'name and password required'})), 404

    user = user_service.get_by_name_password(name=name, password=password)
    if user:
        token = user_service.encode_auth_token(user['name']).decode('utf8')
        return token
    else:
        return make_response(jsonify({'status': 'fail', 'message': 'bad email or password'})), 404


if __name__ == '__main__':
    app.run()
