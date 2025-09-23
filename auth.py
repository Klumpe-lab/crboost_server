import pam

class AuthService:
    """Handles user authentication using the system's PAM service."""

    def __init__(self):
        self._pam = pam.pam()

    def authenticate(self, username: str, password: str) -> bool:
        """
        Checks a username and password against the system's user accounts.
        
        Returns:
            bool: True if authentication is successful, False otherwise.
        """
        try:
            return self._pam.authenticate(username, password)
        except Exception as e:
            # Log the error for debugging, but don't expose details to the user.
            print(f"PAM Authentication Error for user '{username}': {e}")
            return False