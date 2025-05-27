"""
Custom exceptions for the x-thread-dl tool.
Provides consistent error handling across all modules.
"""

class XThreadDLError(Exception):
    """Base exception for all x-thread-dl errors."""
    pass

class ScrapingError(XThreadDLError):
    """Raised when scraping operations fail."""
    pass

class VideoDownloadError(XThreadDLError):
    """Raised when video download operations fail."""
    pass

class DataParsingError(XThreadDLError):
    """Raised when data parsing operations fail."""
    pass

class ConfigurationError(XThreadDLError):
    """Raised when configuration is invalid."""
    pass

class ValidationError(XThreadDLError):
    """Raised when input validation fails."""
    pass
