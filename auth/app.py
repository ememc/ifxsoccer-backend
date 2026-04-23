import json


def _build_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def lambda_handler(event, context):
    body = event.get("body")

    if isinstance(body, str):
        try:
            body = json.loads(body)
        except json.JSONDecodeError:
            return _build_response(
                400,
                {"message": "El body debe ser un JSON valido."},
            )

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
