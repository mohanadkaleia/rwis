import datetime
import jwt
import config


def encode_auth_token(user_id):
    """
    Generates the Auth Token
    :return: string
    """
    payload = {
        'exp': datetime.datetime.utcnow() + datetime.timedelta(days=0, seconds=5),
        'iat': datetime.datetime.utcnow(),
        'sub': user_id
    }

    try:
        return jwt.encode(
            payload,
            config.server.get('SECRET_KEY'),
            algorithm='HS256'
        )
    except Exception as e:
        return


def decode_auth_token(auth_token):
    try:
        payload = jwt.decode(auth_token, config.server.get('SECRET_KEY'))
        return payload['sub']
    except jwt.ExpiredSignatureError:
        return 'Signature expired. Please log in again.'
    except jwt.InvalidTokenError:
        return 'Invalid token. Please log in again.'
