#!/usr/bin/env python3
"""
configure_env.py
Lee (o crea) el .env y rellena automáticamente:
  - RE2_NETWORK_NAME  (detectado desde docker network ls)
  - AIRFLOW_FERNET_KEY
  - AIRFLOW_SECRET_KEY
"""

import os
import re
import secrets
import shutil
import subprocess

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_FILE     = os.path.join(ROOT, ".env")
ENV_EXAMPLE  = os.path.join(ROOT, ".env.example")


def read_env(path):
    with open(path) as f:
        return f.read()


def set_key(content, key, value):
    pattern = rf"^{re.escape(key)}=.*$"
    replacement = f"{key}={value}"
    if re.search(pattern, content, flags=re.MULTILINE):
        return re.sub(pattern, replacement, content, flags=re.MULTILINE)
    # Si la clave no existe, la agrega al final
    return content.rstrip() + f"\n{replacement}\n"


def get_key(content, key):
    m = re.search(rf"^{re.escape(key)}=(.*)$", content, flags=re.MULTILINE)
    return m.group(1).strip() if m else ""


def detect_re2_network():
    try:
        out = subprocess.check_output(
            ["docker", "network", "ls", "--format", "{{.Name}}"],
            text=True
        )
        for line in out.splitlines():
            if "re2" in line:
                return line.strip()
    except Exception:
        pass
    return ""


def main():
    # 1. Crear .env si no existe
    if not os.path.exists(ENV_FILE):
        shutil.copy(ENV_EXAMPLE, ENV_FILE)
        print("✓ .env creado desde .env.example")

    content = read_env(ENV_FILE)

    # 2. RE2_NETWORK_NAME
    if not get_key(content, "RE2_NETWORK_NAME"):
        network = detect_re2_network()
        if network:
            content = set_key(content, "RE2_NETWORK_NAME", network)
            print(f"✓ RE2_NETWORK_NAME={network}")
        else:
            print("⚠️  No se encontró la red re2 — levantá el Proyecto 1 primero")
    else:
        print(f"✓ RE2_NETWORK_NAME ya definida ({get_key(content, 'RE2_NETWORK_NAME')})")

    # 3. AIRFLOW_FERNET_KEY
    if not get_key(content, "AIRFLOW_FERNET_KEY"):
        from cryptography.fernet import Fernet
        key = Fernet.generate_key().decode()
        content = set_key(content, "AIRFLOW_FERNET_KEY", key)
        print("✓ AIRFLOW_FERNET_KEY generada")
    else:
        print("✓ AIRFLOW_FERNET_KEY ya definida")

    # 4. AIRFLOW_SECRET_KEY
    if not get_key(content, "AIRFLOW_SECRET_KEY"):
        key = secrets.token_hex(32)
        content = set_key(content, "AIRFLOW_SECRET_KEY", key)
        print("✓ AIRFLOW_SECRET_KEY generada")
    else:
        print("✓ AIRFLOW_SECRET_KEY ya definida")

    with open(ENV_FILE, "w") as f:
        f.write(content)


if __name__ == "__main__":
    main()
