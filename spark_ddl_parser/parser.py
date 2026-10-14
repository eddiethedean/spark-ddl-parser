"""Parser for Spark SQL DDL schema strings."""

import re

from .types import (
    ArrayType,
    DataType,
    DecimalType,
    MapType,
    SimpleType,
    StructField,
    StructType,
)

_MAX_NESTING = 128
_TYPE_MAPPING = {
    "string": "string",
    "int": "integer",
    "integer": "integer",
    "long": "long",
    "bigint": "long",
    "double": "double",
    "float": "float",
    "real": "float",
    "boolean": "boolean",
    "bool": "boolean",
    "date": "date",
    "timestamp": "timestamp",
    "timestamp_ltz": "timestamp",
    "timestamp_ntz": "timestamp_ntz",
    "binary": "binary",
    "short": "short",
    "smallint": "short",
    "byte": "byte",
    "tinyint": "byte",
}
_DECIMAL_RE = re.compile(
    r"(?:decimal|dec|numeric)\s*(?:\(\s*([0-9]+)\s*(?:,\s*([0-9]+)\s*)?\))?", re.I
)


def parse_ddl_schema(ddl_string: str) -> StructType:
    """Parse a Spark DDL schema into structured Python objects.

    Supports nested structs, arrays, maps, decimal types, quoted identifiers,
    both space and colon field separators, and ``NOT NULL`` fields.
    """
    if not isinstance(ddl_string, str):
        raise TypeError("DDL schema must be a string")
    return _parse_schema(ddl_string, 0)


def _parse_schema(ddl_string: str, depth: int) -> StructType:
    if depth > _MAX_NESTING:
        raise ValueError(f"Maximum DDL nesting depth ({_MAX_NESTING}) exceeded")
    ddl = ddl_string.strip()
    if not ddl:
        return StructType(type_name="struct", fields=[])

    struct_prefix = _complex_prefix_length(ddl, "struct")
    if struct_prefix is not None:
        if not ddl.endswith(">"):
            raise ValueError(f"Invalid struct type: {ddl}")
        ddl = ddl[struct_prefix:-1].strip()

    _validate_delimiters(ddl)
    parts = _split_top_level(ddl, ",")
    if not parts or any(not part.strip() for part in parts):
        raise ValueError("Invalid field definition: empty field")
    return StructType(
        type_name="struct",
        fields=[_parse_field(part.strip(), depth) for part in parts],
    )


def _scan(s: str):
    """Yield (index, character, angle depth, paren depth) outside quoted names."""
    angle = paren = 0
    quoted = False
    i = 0
    while i < len(s):
        char = s[i]
        if char == "`":
            if quoted and i + 1 < len(s) and s[i + 1] == "`":
                i += 2
                continue
            quoted = not quoted
            i += 1
            continue
        if not quoted:
            if char == "<":
                angle += 1
            elif char == ">":
                angle -= 1
            elif char == "(":
                paren += 1
            elif char == ")":
                paren -= 1
            yield i, char, angle, paren
        i += 1
    if quoted:
        raise ValueError("Invalid field definition: unterminated backtick identifier")


def _validate_delimiters(s: str) -> None:
    stack = []
    quoted = False
    i = 0
    while i < len(s):
        char = s[i]
        if char == "`":
            if quoted and i + 1 < len(s) and s[i + 1] == "`":
                i += 2
                continue
            quoted = not quoted
        elif not quoted:
            if char in "<(":
                stack.append(char)
            elif char in ">)":
                expected = "<" if char == ">" else "("
                if not stack or stack.pop() != expected:
                    raise ValueError(f"Unbalanced delimiter: extra '{char}'")
        i += 1
    if quoted:
        raise ValueError("Invalid field definition: unterminated backtick identifier")
    if stack:
        char = stack[-1]
        if char == "<":
            raise ValueError("Unbalanced angle brackets: missing '>'")
        raise ValueError("Unbalanced parentheses: missing ')'")


def _split_top_level(s: str, separator: str) -> list[str]:
    positions = [
        i
        for i, char, angle, paren in _scan(s)
        if char == separator and angle == 0 and paren == 0
    ]
    parts = []
    start = 0
    for pos in positions:
        parts.append(s[start:pos])
        start = pos + 1
    parts.append(s[start:])
    return parts


def _top_level_field_separator(field: str) -> int | None:
    for i, char, angle, paren in _scan(field):
        if angle == 0 and paren == 0 and (char == ":" or char.isspace()):
            return i
    return None


def _parse_field(field_str: str, depth: int) -> StructField:
    separator = _top_level_field_separator(field_str)
    if separator is None:
        raise ValueError(f"Invalid field definition: {field_str}")
    name = field_str[:separator].strip()
    type_start = separator
    if field_str[type_start] == ":":
        type_start += 1
    else:
        while type_start < len(field_str) and field_str[type_start].isspace():
            type_start += 1
        if type_start < len(field_str) and field_str[type_start] == ":":
            type_start += 1
    while type_start < len(field_str) and field_str[type_start].isspace():
        type_start += 1
    type_str = field_str[type_start:].strip()
    if not name or not type_str:
        raise ValueError(f"Invalid field definition: {field_str}")

    if name.startswith("`") or name.endswith("`"):
        if len(name) < 2 or not (name.startswith("`") and name.endswith("`")):
            raise ValueError(f"Invalid field definition: {field_str}")
        quoted_name = name[1:-1]
        name_characters = []
        index = 0
        while index < len(quoted_name):
            if quoted_name[index] == "`":
                if index + 1 >= len(quoted_name) or quoted_name[index + 1] != "`":
                    raise ValueError(f"Invalid field definition: {field_str}")
                name_characters.append("`")
                index += 2
            else:
                name_characters.append(quoted_name[index])
                index += 1
        name = "".join(name_characters)
    elif "`" in name or any(char.isspace() for char in name):
        raise ValueError(f"Invalid field definition: {field_str}")

    nullable = True
    suffix = re.search(r"\s+NOT\s+NULL\s*$", type_str, re.I)
    if suffix:
        nullable = False
        type_str = type_str[: suffix.start()].rstrip()
    return StructField(
        name=name, data_type=_parse_type(type_str, depth), nullable=nullable
    )


def _parse_type(type_str: str, depth: int) -> DataType:
    if depth > _MAX_NESTING:
        raise ValueError(f"Maximum DDL nesting depth ({_MAX_NESTING}) exceeded")
    type_str = type_str.strip()
    if not type_str:
        raise ValueError("Invalid type: empty type string")
    lowered = type_str.casefold()

    match = _DECIMAL_RE.fullmatch(type_str)
    if match:
        precision = int(match.group(1)) if match.group(1) else 10
        scale = int(match.group(2)) if match.group(2) else 0
        if not 1 <= precision <= 38 or not 0 <= scale <= precision:
            raise ValueError(f"Invalid decimal type: {type_str}")
        return DecimalType(type_name="decimal", precision=precision, scale=scale)
    if lowered.startswith(("decimal", "numeric", "dec (", "dec(")) or lowered == "dec":
        raise ValueError(f"Invalid decimal type: {type_str}")

    complex_prefix = _complex_prefix_length(type_str, "array")
    if complex_prefix is not None:
        inner = _complex_inner(type_str, "array", complex_prefix)
        if not inner or len(_split_top_level(inner, ",")) != 1:
            raise ValueError(f"Invalid array type: {type_str}")
        return ArrayType(type_name="array", element_type=_parse_type(inner, depth + 1))
    complex_prefix = _complex_prefix_length(type_str, "map")
    if complex_prefix is not None:
        inner = _complex_inner(type_str, "map", complex_prefix)
        parts = _split_top_level(inner, ",")
        if len(parts) != 2 or not all(part.strip() for part in parts):
            raise ValueError(f"Invalid map type: {type_str}")
        return MapType(
            type_name="map",
            key_type=_parse_type(parts[0], depth + 1),
            value_type=_parse_type(parts[1], depth + 1),
        )
    complex_prefix = _complex_prefix_length(type_str, "struct")
    if complex_prefix is not None:
        inner = _complex_inner(type_str, "struct", complex_prefix)
        if not inner.strip():
            raise ValueError(f"Invalid struct type: {type_str}")
        return _parse_schema(inner, depth + 1)

    if lowered in ("decimal", "dec", "numeric"):
        return DecimalType(type_name="decimal")
    if lowered in _TYPE_MAPPING:
        return SimpleType(type_name=_TYPE_MAPPING[lowered])
    if any(char.isspace() for char in type_str):
        raise ValueError(f"Invalid field definition: {type_str}")
    raise ValueError(f"Unsupported data type: {type_str}")


def _complex_prefix_length(type_str: str, kind: str) -> int | None:
    match = re.match(rf"{kind}\s*<", type_str, re.I)
    return match.end() if match else None


def _complex_inner(type_str: str, kind: str, prefix_length: int) -> str:
    if not type_str.endswith(">"):
        raise ValueError(f"Invalid {kind} type: {type_str}")
    inner = type_str[prefix_length:-1]
    _validate_delimiters(type_str)
    return inner.strip()
