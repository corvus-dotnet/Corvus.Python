import polars as pl
from pandera.polars import DataFrameModel
from polars import Date, Float64, Int64, String, Datetime, Boolean, Decimal
from typing import Type, Set, Dict, Any


def pandera_to_polars_schema(cls: Type[DataFrameModel]) -> pl.Schema:
    """
    Generates a Polars Schema from a DataFrameModel class and its base classes, using their annotations.

    This function iterates over the Method Resolution Order (MRO) of the provided class. It examines each base class
    that is a subclass of DataFrameModel (excluding DataFrameModel itself). For each, it extracts annotated fields,
    maps them to Polars DataTypes, and creates a Field for each. The function ensures uniqueness of each field in
    the resulting schema.

    Parameters
    ----------
    cls : DataFrameModel
        The class inheriting from DataFrameModel for which the Polars schema is to be generated.

    Returns
    -------
    pl.Schema
        A Polars Schema with Fields for each annotated field in the class hierarchy.

    Notes
    -----
    The function uses guard clauses for reduced nesting and improved readability.

    Examples
    --------
    >>> import pandera as pa
    >>> import polars as pl
    >>> class MyDataFrameModel(pa.DataFrameModel):
    ...     name: pa.typing.Series[str]
    ...     age: pa.typing.Series[int]
    >>> schema = pandera_to_polars_schema(MyDataFrameModel)
    >>> print(schema)
    {'name': String, 'age': Int64}
    """
    schema_dict: Dict[str, Any] = {}
    added_fields: Set[str] = set()

    type_mapping: dict = {
        str: String,
        "String": String,
        int: Int64,
        float: Float64,
        bool: Boolean,
        "Timestamp": Datetime("us"),
        "datetime": Datetime("us"),
        "Datetime": Datetime("us"),
        "Date": Date,
        "date": Date,
        "Decimal": Decimal,
        "decimal": Decimal,
        # Add more mappings as needed for other pandera types like Categorical, etc.
    }

    for base in cls.__mro__:
        if not issubclass(base, DataFrameModel) or base == DataFrameModel:
            continue

        for attr_name, attr_type in base.__annotations__.items():
            # Extract the inner type from pandera.typing.Series (e.g., Series[str] -> str)
            if hasattr(attr_type, "__args__") and attr_type.__args__:
                inner_type = attr_type.__args__[0]
            else:
                inner_type = attr_type

            # Determine the key for type_mapping lookup
            # For built-in types (str, int, float, bool), inner_type itself is the key
            # For types like pandera.typing.Timestamp, its __name__ might be 'Timestamp'
            type_key = inner_type.__name__ if hasattr(inner_type, "__name__") else inner_type

            polars_type = type_mapping.get(type_key, None)

            if attr_name in added_fields or polars_type is None:
                # If type is not directly mapped, try to infer from common Python types
                if inner_type is str:
                    polars_type = String
                elif inner_type is int:
                    polars_type = Int64
                elif inner_type is float:
                    polars_type = Float64
                elif inner_type is bool:
                    polars_type = Boolean
                elif isinstance(inner_type, type) and issubclass(inner_type, (type(None),)):  # Handle Optional types
                    continue
                else:
                    print(f"Warning: No direct Polars mapping for Pandera type: {inner_type}")
                    polars_type = String

            if attr_name in added_fields:  # Skip if already added from a base class
                continue

            schema_dict[attr_name] = polars_type
            added_fields.add(attr_name)

    return pl.Schema(schema_dict)
