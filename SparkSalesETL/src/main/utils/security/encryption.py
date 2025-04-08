import os
from cryptography.fernet import Fernet
from pathlib import Path


class AWSCredentialsManager:
    def __init__(self, key_path=None):
        self.root = Path(__file__).resolve().parent.parent.parent.parent.parent
        self.key_path = key_path or os.path.join(self.root, 'secret.key')
        self.key = self._load_or_create_key()

    def _load_or_create_key(self):
        if os.path.exists(self.key_path):
            with open(self.key_path, 'rb') as f:
                return f.read()
        else:
            new_key = Fernet.generate_key()
            with open(self.key_path, 'wb') as f:
                f.write(new_key)
            return new_key


    def encrypt_credentials(self, access_key, secret_key):
        output_path = os.path.join(self.root, "src/main/config/encrypted_credentials.enc")
        cipher = Fernet(self.key)
        encrypted = cipher.encrypt(f'{access_key}:{secret_key}'.encode())
        with open(output_path, 'wb') as f:
            f.write(encrypted)

    def decrypt_credentials(self):
        input_path = os.path.join(self.root, "src/main/config/encrypted_credentials.enc")
        cipher = Fernet(self.key)
        with open(input_path, 'rb') as f:
            encrypted = f.read()
        decrypted = cipher.decrypt(encrypted).decode()
        return decrypted.split(':')