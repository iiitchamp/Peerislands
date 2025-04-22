class TransformationError(Exception):
    """Base class for transformation errors."""
    pass

class InvalidConfigurationError(TransformationError):
    """Raised when configuration is invalid."""
    pass

class UnsupportedTransformationError(TransformationError):
    """Raised when an unsupported transformation is requested."""
    pass

class MissingParameterError(TransformationError):
    """Raised when required parameters are missing."""
    pass

class ActionExecutionError(TransformationError):
    """Raised when an action fails to execute."""
    pass
