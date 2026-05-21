import base64
import json


def _build_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "GET,POST,PUT,OPTIONS",
        },
        "body": json.dumps(body),
    }


def _get_http_method(event, default="POST"):
    try:
        outer_body = _get_body_payload(event)

        if isinstance(outer_body, dict):
            nested_method = outer_body.get("httpMethod")

            if isinstance(nested_method, str) and nested_method.strip():
                return nested_method.strip().upper()

    except Exception:
        pass

    method = event.get("httpMethod") or default
    return method.upper()


def _get_body_payload(event):
    """
    Soporta:
    1. API Gateway REST/HTTP API:
       event["body"] = '{"username":"...", "password":"..."}'

    2. Lambda Console directo:
       {
         "username": "...",
         "password": "..."
       }
    """

    body = event.get("body")

    if body is None:
        return event

    if event.get("isBase64Encoded"):
        if not isinstance(body, str):
            raise ValueError("El body en base64 debe ser texto.")

        try:
            body = base64.b64decode(body).decode("utf-8")
        except (ValueError, UnicodeDecodeError) as error:
            raise ValueError("El body en base64 no es valido.") from error

    if isinstance(body, dict):
        return body

    if not isinstance(body, str):
        raise ValueError("El body debe ser texto JSON.")

    try:
        return json.loads(body)
    except json.JSONDecodeError as error:
        raise ValueError("El body no contiene un JSON valido.") from error


def lambda_handler(event, context):
    method = _get_http_method(event)

    if method == "OPTIONS":
        return _build_response(200, {"message": "CORS preflight OK."})

    try:
        outer_body = _get_body_payload(event)
    except ValueError as error:
        return _build_response(400, {"message": str(error)})

    if not isinstance(outer_body, dict):
        return _build_response(
            400,
            {"message": "El body externo debe ser un objeto JSON."},
        )

    inner_body = outer_body.get("body")

    if isinstance(inner_body, str):
        try:
            body = json.loads(inner_body)
        except json.JSONDecodeError:
            return _build_response(
                400,
                {"message": "El body interno no contiene JSON valido."},
            )

        if not isinstance(body, dict):
            return _build_response(
                400,
                {"message": "El body interno debe ser un objeto JSON."},
            )
    elif isinstance(inner_body, dict):
        body = inner_body
    else:
        body = outer_body

    if not isinstance(body, dict):
        return _build_response(
            400,
            {"message": "Debes enviar username y password."},
        )

    username = body.get("username")
    password = body.get("password")

    if not username or not password:
        return _build_response(
            400,
            {"message": "Los campos username y password son obligatorios."},
        )

    # Validacion inicial temporal. Podemos reemplazarla luego por BD, Cognito o un servicio interno.
    valid_users = {
        "admin": "ifx123",
        "operador": "segura456",
    }

    if valid_users.get(username) != password:
        return _build_response(
            401,
            {"message": "Credenciales invalidas.", "authenticated": False},
        )

    return _build_response(
        200,
        {
            "message": "Usuario validado correctamente.",
            "authenticated": True,
            "user": {"username": username},
        },
    )
