import base64
import json
import os
from decimal import Decimal

import boto3
from botocore.exceptions import BotoCoreError, ClientError


dynamodb = boto3.resource("dynamodb", region_name="us-west-1")

REQUIRED_MENU_FIELDS = (
    "menu_id",
    "menu_header",
)

UPDATABLE_MENU_FIELDS = (
    "menu_header",
)

REQUIRED_HEADER_FIELDS = (
    "header_id",
    "header_text",
    "header_enabled",
    "header_order",
    "header_call",
    "header_target",
    "menu_section",
)

REQUIRED_SECTION_FIELDS = (
    "section_id",
    "section_title",
    "section_image",
    "section_call",
    "section_target",
    "section_order",
    "section_detail",
    "menu_detail",
)

REQUIRED_DETAIL_FIELDS = (
    "detail_id",
    "detail_title",
    "section_image",
    "section_call",
    "section_target",
    "section_order",
)


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            if obj % 1 == 0:
                return int(obj)
            return float(obj)
        return super().default(obj)


def _build_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
        },
        "body": json.dumps(body, cls=DecimalEncoder),
    }


def _get_http_method(event, default="GET"):
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


def _get_path_parameters(event):
    try:
        outer_body = _get_body_payload(event)

        if isinstance(outer_body, dict):
            nested_path_parameters = outer_body.get("pathParameters")

            if isinstance(nested_path_parameters, dict):
                return nested_path_parameters
    except Exception:
        pass

    path_parameters = event.get("pathParameters")
    return path_parameters if isinstance(path_parameters, dict) else {}


def _get_body_payload(event):
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


def _get_table():
    table_name = os.environ.get("MENU_TABLE_NAME", "menu")
    if not table_name:
        raise ValueError("Falta la variable de entorno MENU_TABLE_NAME.")
    return dynamodb.Table(table_name)


def _parse_body(event):
    outer_body = _get_body_payload(event)

    if outer_body is None:
        return {}

    if not isinstance(outer_body, dict):
        raise ValueError("El body externo debe ser un objeto JSON.")

    inner_body = outer_body.get("body")

    if isinstance(inner_body, str):
        try:
            payload = json.loads(inner_body)
        except json.JSONDecodeError as error:
            raise ValueError("El body interno no contiene JSON valido.") from error

        if not isinstance(payload, dict):
            raise ValueError("El body interno debe ser un objeto JSON.")

        return payload

    if isinstance(inner_body, dict):
        return inner_body

    return outer_body


def _validate_required_fields(body):
    missing_fields = [field for field in REQUIRED_MENU_FIELDS if field not in body]
    if missing_fields:
        raise ValueError(
            f"Faltan campos obligatorios: {', '.join(missing_fields)}."
        )

    _validate_menu_field_types(body)


def _validate_required_keys(item, required_keys, path):
    missing_keys = [key for key in required_keys if key not in item]
    if missing_keys:
        raise ValueError(f"Faltan campos en {path}: {', '.join(missing_keys)}.")


def _validate_menu_field_types(fields):
    if "menu_header" not in fields:
        return

    headers = fields["menu_header"]
    if not isinstance(headers, list):
        raise ValueError("El campo menu_header debe ser una lista.")

    for header_index, header in enumerate(headers):
        header_path = f"menu_header[{header_index}]"

        if not isinstance(header, dict):
            raise ValueError(f"El campo {header_path} debe ser un objeto.")

        _validate_required_keys(header, REQUIRED_HEADER_FIELDS, header_path)

        if not isinstance(header["header_enabled"], bool):
            raise ValueError(
                f"El campo {header_path}.header_enabled debe ser booleano."
            )

        sections = header["menu_section"]
        if not isinstance(sections, list):
            raise ValueError(f"El campo {header_path}.menu_section debe ser una lista.")

        for section_index, section in enumerate(sections):
            section_path = f"{header_path}.menu_section[{section_index}]"

            if not isinstance(section, dict):
                raise ValueError(f"El campo {section_path} debe ser un objeto.")

            _validate_required_keys(section, REQUIRED_SECTION_FIELDS, section_path)

            if not isinstance(section["section_detail"], bool):
                raise ValueError(
                    f"El campo {section_path}.section_detail debe ser booleano."
                )

            details = section["menu_detail"]
            if not isinstance(details, list):
                raise ValueError(f"El campo {section_path}.menu_detail debe ser una lista.")

            for detail_index, detail in enumerate(details):
                detail_path = f"{section_path}.menu_detail[{detail_index}]"

                if not isinstance(detail, dict):
                    raise ValueError(f"El campo {detail_path} debe ser un objeto.")

                _validate_required_keys(detail, REQUIRED_DETAIL_FIELDS, detail_path)


def _create_menu(table, body):
    _validate_required_fields(body)

    menu = {field: body[field] for field in REQUIRED_MENU_FIELDS}

    table.put_item(
        Item=menu,
        ConditionExpression="attribute_not_exists(menu_id)",
    )

    return _build_response(
        201,
        {
            "message": "Menu creado correctamente.",
            "menu": menu,
        },
    )


def _update_menu(table, menu_id, body):
    if not menu_id:
        raise ValueError("Debes enviar menu_id en la URL.")

    update_fields = {key: body[key] for key in UPDATABLE_MENU_FIELDS if key in body}

    if not update_fields:
        raise ValueError("Debes enviar al menos un campo para actualizar.")

    _validate_menu_field_types(update_fields)

    expression_attribute_names = {}
    expression_attribute_values = {}
    update_parts = []

    for index, (field_name, field_value) in enumerate(update_fields.items(), start=1):
        name_key = f"#field{index}"
        value_key = f":value{index}"
        expression_attribute_names[name_key] = field_name
        expression_attribute_values[value_key] = field_value
        update_parts.append(f"{name_key} = {value_key}")

    response = table.update_item(
        Key={"menu_id": menu_id},
        UpdateExpression="SET " + ", ".join(update_parts),
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values,
        ConditionExpression="attribute_exists(menu_id)",
        ReturnValues="ALL_NEW",
    )

    return _build_response(
        200,
        {
            "message": "Menu actualizado correctamente.",
            "menu": response.get("Attributes", {}),
        },
    )


def _get_menu(table, menu_id):
    response = table.get_item(Key={"menu_id": menu_id})
    item = response.get("Item")

    if not item:
        return _build_response(
            404,
            {"message": "Menu no encontrado.", "menu_id": menu_id},
        )

    return _build_response(
        200,
        {
            "message": "Menu obtenido correctamente.",
            "menu": item,
        },
    )


def _list_menus(table):
    items = []
    scan_kwargs = {}

    while True:
        response = table.scan(**scan_kwargs)
        items.extend(response.get("Items", []))

        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

        scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

    return _build_response(
        200,
        {
            "message": "Menus obtenidos correctamente.",
            "count": len(items),
            "menus": items,
        },
    )


def _delete_menu(table, menu_id):
    if not menu_id:
        raise ValueError("Debes enviar menu_id en la URL.")

    response = table.delete_item(
        Key={"menu_id": menu_id},
        ConditionExpression="attribute_exists(menu_id)",
        ReturnValues="ALL_OLD",
    )

    return _build_response(
        200,
        {
            "message": "Menu eliminado correctamente.",
            "menu": response.get("Attributes", {}),
        },
    )


def lambda_handler(event, context):
    method = _get_http_method(event)
    menu_id = _get_path_parameters(event).get("menu_id")

    try:
        if method == "OPTIONS":
            return _build_response(200, {"message": "CORS preflight OK."})

        table = _get_table()

        if method == "GET":
            if menu_id:
                return _get_menu(table, menu_id)
            return _list_menus(table)

        if method == "POST":
            body = _parse_body(event)
            return _create_menu(table, body)

        if method == "PUT":
            body = _parse_body(event)
            return _update_menu(table, menu_id, body)

        if method == "DELETE":
            return _delete_menu(table, menu_id)

        return _build_response(
            405,
            {"message": f"Metodo {method} no soportado."},
        )
    except ValueError as error:
        return _build_response(400, {"message": str(error)})
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code")

        if error_code == "ConditionalCheckFailedException":
            if method == "POST":
                return _build_response(
                    409,
                    {"message": "Ya existe un menu con ese menu_id."},
                )

            if method in ("PUT", "DELETE"):
                return _build_response(
                    404,
                    {"message": "Menu no encontrado.", "menu_id": menu_id},
                )

        return _build_response(
            500,
            {
                "message": "Error consultando DynamoDB.",
                "details": str(error),
            },
        )
    except BotoCoreError as error:
        return _build_response(
            500,
            {
                "message": "Error consultando DynamoDB.",
                "details": str(error),
            },
        )
