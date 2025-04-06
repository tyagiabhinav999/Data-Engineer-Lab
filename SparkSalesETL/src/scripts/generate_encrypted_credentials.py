from src.main.utils.security.encrypt_decrypt import AWSCredentialsManager

if __name__=='__main__':
    manager = AWSCredentialsManager()

    access_key = input("Enter AWS Access Key ID: ").strip()
    secret_key = input("Enter AWS Secret Access Key: ").strip()

    manager.encrypt_credentials(access_key, secret_key)
    print("Credentials encrypted to config/encrypted_credentials.enc")
