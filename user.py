import datetime
import jwt
import config
from tinydb import Query, TinyDB
import bcrypt

db = TinyDB('db.json')
table = db.table('user')


class InvalidToken(jwt.InvalidTokenError):
    pass


class ExpiredToken(jwt.ExpiredSignatureError):
    pass


def __normalize(password):
    password = password.encode('utf8')
    password = bcrypt.hashpw(password, bcrypt.gensalt())
    return password.decode('utf8')


def create(name, password):
    password = __normalize(password)
    return table.insert({'name': name, 'password': password})


def update(name=None, password=None, token=None):
    data = {}
    if name:
        data['name'] = name

    if password:
        password = __normalize(password)
        data['password'] = password

    if token:
        data['token'] = token

    table.update(data, Query().name == name)


def get_by_name(name):
    result = table.search(Query().name == name)
    if not result:
        return None
    return result[0]


def get_by_name_password(name, password):
    print(name)
    user = get_by_name(name)
    valid = user['password'].encode('utf8') == bcrypt.hashpw(password.encode('utf8'), user['password'].encode('utf8'))
    if valid:
        return user


def authenticate(name, token):
    return table.search(Query().name == name & Query().token == token)


def encode_auth_token(user_id):
    """
    Generates the Auth Token
    :return: string
    """
    payload = {
        'exp': datetime.datetime.utcnow() + datetime.timedelta(days=365),
        'iat': datetime.datetime.utcnow(),
        'sub': user_id
    }

    return jwt.encode(payload, config.server.get('SECRET_KEY'), algorithm='HS256')


def decode_auth_token(auth_token):
    try:
        payload = jwt.decode(auth_token, config.server.get('SECRET_KEY'))
        return payload['sub']
    except jwt.ExpiredSignatureError:
        raise ExpiredToken('Signature expired. Please log in again.')
    except jwt.InvalidTokenError:
        raise InvalidToken('Invalid token. Please log in again.')


if __name__ == '__main__':
    create('st0', '123456')
