class PlaceHrefParseError(ValueError):
    pass


class PlaceMissingAddress(ValueError):
    pass


class ScrollEndNotReachedError(RuntimeError):
    pass


class OpenTimeFormatError(ValueError):
    """Raised when open time table does not contain 7 day rows."""
    pass
