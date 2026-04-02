# ui/utils.py


def snake_to_title(s: str) -> str:
    """Convert 'some_snake_name' to 'Some Snake Name'."""
    return " ".join(word.capitalize() for word in s.split("_"))
