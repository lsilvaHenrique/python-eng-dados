import requests

try:
    response = requests.get('https://api.github.com')
    response.raise_for_status()
    print("Requisição bem sucedida!")
except requests.exceptions.RequestException as e:
    print(f"Erro ao fazer requisição: {e}")
    response = None

response = requests.get('https://api.github.com')
print(response.status_code)
input("Press Enter to continue...")
print(response.json())
input("Press Enter to continue...")
print(response.headers)
input("Press Enter to continue...")
print(response.text)

