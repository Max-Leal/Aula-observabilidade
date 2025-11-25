FROM python:3.10-alpine

# Instalação de dependências do sistema operacional para PyMySQL
# (Embora PyMySQL seja puro Python, é bom ter certeza)
RUN apk update && apk add --no-cache gcc musl-dev

WORKDIR /app

COPY requirements.txt .
COPY load_generator.py .

# Instala as dependências Python (PyMySQL)
RUN pip install --no-cache-dir -r requirements.txt

# Comando de execução
CMD ["python", "load_generator.py"]