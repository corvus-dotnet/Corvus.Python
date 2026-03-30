class EmailError(Exception):
    def __init__(self, error: str) -> None:
        super().__init__(f"There was an error sending the email message: '{error}'")
