#!/usr/bin/env python3
import socket
import argparse
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from nicegui import ui

from ui import create_ui_router
from backend import CryoBoostBackend
from auth import AuthService

from datetime import datetime, timedelta, timezone
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

def generate_self_signed_cert(cert_path: Path, key_path: Path):
    """
    Generates a self-signed SSL certificate and a private key if they don't exist.
    """
    if cert_path.exists() and key_path.exists():
        return

    print("Generating a new self-signed SSL certificate...")
    
    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )

    with open(key_path, "wb") as f:
        f.write(key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption()
        ))
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, u"US"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, u"CA"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, u"Vienna/IMP"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, u"CryoBoost Self-Signed"),
        x509.NameAttribute(NameOID.COMMON_NAME, socket.gethostname()),
    ])

    cert = x509.CertificateBuilder().subject_name(
        subject
    ).issuer_name(
        issuer
    ).public_key(
        key.public_key()
    ).serial_number(
        x509.random_serial_number()
    ).not_valid_before(
        datetime.now(timezone.utc)
    ).not_valid_after(
        datetime.now(timezone.utc) + timedelta(days=365)
    ).add_extension(
        x509.SubjectAlternativeName([x509.DNSName(u"localhost")]),
        critical=False,
    ).sign(key, hashes.SHA256(), default_backend())

    with open(cert_path, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
    
    print(f"Certificate and key saved to {cert_path} and {key_path}")


def setup_app():
    """Configures and returns the FastAPI app."""
    app = FastAPI()
    app.mount("/static", StaticFiles(directory="static"), name="static")

    ui.add_head_html('''
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&display=swap" rel="stylesheet">
        <link rel="stylesheet" href="/static/main.css">
    ''')
    
    backend = CryoBoostBackend(Path.cwd())
    auth_service = AuthService()
    create_ui_router(backend, auth_service)

    ui.run_with(app, title="CryoBoost Server", storage_secret="A_REALLY_SECRET_KEY_CHANGE_ME")

    return app

def get_local_ip():
    """Get the local IP address"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "localhost"

if __name__ in {"__main__", "__mp_main__"}:
    auth_dir = Path("auth")
    auth_dir.mkdir(exist_ok=True)

    key_file = auth_dir / "key.pem"
    cert_file = auth_dir / "cert.pem"
    generate_self_signed_cert(cert_file, key_file)

    app = setup_app()

    parser = argparse.ArgumentParser(description='CryoBoost Server')
    parser.add_argument('--port', type=int, default=8081, help='Port to run server on')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to bind to')
    args = parser.parse_args()

    local_ip = get_local_ip()
    hostname = socket.gethostname()
    
    print("CryoBoost Server Starting")
    print(f"Access URLs:")
    print(f"  Local:   https://localhost:{args.port}")
    print(f"  Network: https://{local_ip}:{args.port}")
    print("\nTo access from another machine, use an SSH tunnel:")
    print(f"ssh -L 8081:{hostname}:{args.port} [YOUR_USERNAME]@{hostname}")
    print("-" * 30)
    print("NOTE: You will see a browser security warning. Please accept it to proceed.")

    uvicorn.run(
        app, 
        host=args.host, 
        port=args.port,
        ssl_keyfile=str(key_file),
        ssl_certfile=str(cert_file)
    )